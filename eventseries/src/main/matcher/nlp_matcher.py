import logging
from typing import List, Optional, Tuple, Dict

import pandas as pd
import spacy

from eventseries.src.main.matcher.acronym_matcher import AcronymMatch
from eventseries.src.main.matcher.naive_word2vec_matcher import NaiveWord2VecMatch
from eventseries.src.main.matcher.ngram_matcher import NgramMatch
from eventseries.src.main.matcher.phrase_matcher import PhraseMatch
from eventseries.src.main.matcher.tfidf_matcher import TfIdfMatch
from eventseries.src.main.matcher.word2vec_matcher import Word2VecMatch
from eventseries.src.main.repository.completions import Match, FullMatch, NameMatch
from eventseries.src.main.repository.completions import get_titles_from_match
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventSeries,
    QID,
)
from eventseries.src.main.util.fetch_miscellaneous_event_series import FetchEventSeries


def create_training_test_dataset(previous_found_matches: List[Match]) -> pd.DataFrame:
    """We create a training and test dataset out of the matches from:
    DBLP (Matches from event to event series for conferences)
    Existing matches from Wikidata
    Full matches from wikidata titles/labels/CEUR-WS event titles with event series titles"""
    match_titles = [get_titles_from_match(match) for match in previous_found_matches]
    list_of_dicts = [{"event": event, "series": series} for event, series in match_titles]
    # Convert the dictionary to a DataFrame
    return pd.DataFrame(list_of_dicts, columns=["event", "series"])


def read_miscellaneous_event_series() -> List[str]:
    fetch_event_series = FetchEventSeries()
    return fetch_event_series.get_bard_event_series()


def spacy_package_exists():
    if not spacy.util.is_package("en_core_web_sm"):
        logging.error(
            "Could not find spacy package 'en_core_web_sm' please run"
            " 'python -m spacy download en_core_web_sm'"
        )
        return False
    return True


class NlpMatcher:
    """We use the DBLP matched events with event series
    to test our algorithms and then apply them to wikidata events"""

    def __init__(self, matches_df: pd.DataFrame) -> None:
        # Dataframe with "event" and "series" as column containing titles of found matches.
        self.train_test_set: pd.DataFrame = matches_df

    def match(
        self,
        unmatched_events: List[WikiDataEvent],
        event_series: List[WikiDataEventSeries],
        additional_series: Optional[List[str]] = None,
        try_multiple_skip_grams=False,
    ) -> List[Match]:
        if additional_series is None:
            additional_series = read_miscellaneous_event_series()

        proxy_event_series = [
            WikiDataEventSeries(label=title, title=title, qid=QID("Q0"))
            for title in additional_series
        ]
        all_event_series: List[WikiDataEventSeries] = event_series + proxy_event_series

        phrase_matches = []
        acronym_matches = []
        if spacy_package_exists():
            phrase_matcher = PhraseMatch(matches_df=self.train_test_set)
            phrase_matcher.test_accuracy()
            phrase_matches = phrase_matcher.wikidata_match(unmatched_events, all_event_series)
            logging.info("Found %s matched through phrase-matching.", len(phrase_matches))

            acronym_matcher = AcronymMatch(self.train_test_set)
            acronym_matcher.test_accuracy()
            acronym_matches = acronym_matcher.wikidata_match(unmatched_events, all_event_series)
            logging.info("Found %s matched through acronym-matching.", len(acronym_matches))

        ngram_matcher = NgramMatch(matches_df=self.train_test_set)
        ngram_matches = ngram_matcher.match_events_to_series(
            event_list=unmatched_events, series_list=all_event_series
        )
        logging.info("Found %s matched through n-grams.", len(ngram_matches))

        tfidf_matches = TfIdfMatch(self.train_test_set).wikidata_match(
            unmatched_events, all_event_series
        )
        logging.info("Found %s matched through tf-idf-matches.", len(tfidf_matches))

        naive_word2vec_matcher = NaiveWord2VecMatch(self.train_test_set)
        naive_matches = naive_word2vec_matcher.wikidata_match(unmatched_events, all_event_series)
        remaining_events = unmatched_events
        for match in naive_matches:
            remaining_events.remove(match.event)

        # Since our training data is less we start with skip grams = 0 i.e. - CBOW
        word2vec_matcher_sg_0 = Word2VecMatch(self.train_test_set, 0)
        word2vec_sg_0_matches = word2vec_matcher_sg_0.wikidata_match(
            unmatched_events, all_event_series
        )

        word2vec_sg_1_matches = []
        if try_multiple_skip_grams:  # Example runs showed that both find the same matches
            word2vec_matcher_sg_1 = Word2VecMatch(self.train_test_set, 1)
            word2vec_sg_1_matches = word2vec_matcher_sg_1.wikidata_match(
                unmatched_events, all_event_series
            )
            logging.info("Matches for Word2VecMatch:")
            logging.info(
                "Found %s matches with skip_grams=0 and %s for skp_grams=1",
                len(word2vec_sg_0_matches),
                len(word2vec_sg_1_matches),
            )

        all_found_matches: List[FullMatch] = (
            phrase_matches
            + acronym_matches
            + ngram_matches
            + tfidf_matches
            + naive_matches
            + word2vec_sg_0_matches
            + word2vec_sg_1_matches
        )
        logging.info(
            "In total %s matches were reported (possibly duplicate).", len(all_found_matches)
        )

        cross_validates_matches = self.cross_validate_and_merge(
            all_found_matches, required_to_pass=3
        )
        logging.info(
            "After cross validation with n = 3, %s unique matches were left",
            len(cross_validates_matches),
        )

        # Filter out the proxy event series and convert to NameMatch
        final_matches: List[Match] = []
        for found_match in cross_validates_matches:
            if found_match.series.qid == QID("Q0"):
                final_matches.append(
                    NameMatch(
                        event=found_match.event,
                        series=found_match.series.title,
                        found_by=found_match.found_by,
                    )
                )
            else:
                final_matches.append(found_match)

        return final_matches

    def cross_validate_and_merge(
        self, matches: List[FullMatch], required_to_pass: int
    ) -> List[FullMatch]:
        """Group matches by qids of event and series.
        Filter out all matches that were found by fewer than required_to_pass matches."""
        by_qids: Dict[Tuple[QID, QID], List[FullMatch]] = {}

        def get_key(match: FullMatch):
            return (match.event.qid, match.series.qid)

        for match in matches:
            key = get_key(match)
            if key in by_qids:
                by_qids[key].append(match)
            else:
                by_qids[key] = [match]

        # Frequency of how often the match was found to how often frequency occurred
        # Only done for insights.
        by_frequency = {}
        for key, match_list in by_qids.items():
            freq = len(match_list)
            by_frequency[freq] = by_frequency.get(freq, 0) + 1
        for freqency, occurence in by_frequency.items():
            logging.info("%s matches were found %s many times", occurence, freqency)

        # Filter out
        passed_matches = {
            key: value for key, value in by_qids.items() if len(value) >= required_to_pass
        }
        # Merge duplicates
        merged: List[FullMatch] = []
        for same_matches in passed_matches.values():
            first: FullMatch = same_matches[0]
            all_label: str = "+".join([m.found_by for m in same_matches])
            merged.append(FullMatch(event=first.event, series=first.series, found_by=all_label))
        return merged
