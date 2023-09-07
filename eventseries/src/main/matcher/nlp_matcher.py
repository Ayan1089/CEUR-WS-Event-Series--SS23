import logging
from typing import List

import pandas as pd

from eventseries.src.main.matcher.naive_word2vec_matcher import NaiveWord2VecMatch
from eventseries.src.main.matcher.word2vec_matcher import Word2VecMatch
from eventseries.src.main.repository.completions import Match
from eventseries.src.main.repository.completions import get_titles_from_match
from eventseries.src.main.repository.wikidata_dataclasses import WikiDataEvent, WikiDataEventSeries
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


class NlpMatcher:
    """We use the DBLP matched events with event series
    to test our algorithms and then apply them to wikidata events"""

    def __init__(self, matches_df: pd.DataFrame) -> None:
        # Dataframe with "event" and "series" as column containing titles of found matches.
        self.train_test_set: pd.DataFrame = matches_df

    def match(
        self, events: List[WikiDataEvent], event_series: List[WikiDataEventSeries]
    ) -> List[Match]:
        naive_word2vec_matcher = NaiveWord2VecMatch(self.train_test_set)
        naive_matches = naive_word2vec_matcher.wikidata_match(events, event_series)
        remaining_events = events
        for match in naive_matches:
            remaining_events.remove(match.event)

        # Since our training data is less we start with skip grams = 0 i.e. - CBOW
        word2vec_matcher_sg_0 = Word2VecMatch(self.train_test_set, 0)
        word2vec_sg_0_matches = word2vec_matcher_sg_0.wikidata_match(events, event_series)

        word2vec_matcher_sg_1 = Word2VecMatch(self.train_test_set, 1)
        word2vec_sg_1_matches = word2vec_matcher_sg_1.wikidata_match(events, event_series)
        logging.info("Matches for Word2VecMatch:")
        logging.info(
            "Found %s matches with skip_grams=0 and %s for skp_grams=1",
            len(word2vec_sg_0_matches),
            len(word2vec_sg_1_matches),
        )

        unique_matches = {match.event.qid: match for match in naive_matches}
        for s0_match in word2vec_sg_0_matches:
            if s0_match.event.qid not in unique_matches:
                unique_matches[s0_match.event.qid] = s0_match
        for s1_match in word2vec_sg_1_matches:
            if s1_match.event.qid not in unique_matches:
                unique_matches[s1_match.event.qid] = s1_match

        return list(unique_matches.values())
