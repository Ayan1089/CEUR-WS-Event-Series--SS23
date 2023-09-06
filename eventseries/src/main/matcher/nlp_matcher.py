import json
import os
from typing import List

import pandas as pd

from eventseries.src.main.matcher.acronym_matcher import AcronymMatch
from eventseries.src.main.matcher.naive_word2vec_matcher import NaiveWord2VecMatch
from eventseries.src.main.matcher.ngram_matcher import NgramMatch
from eventseries.src.main.matcher.phrase_matcher import PhraseMatch
from eventseries.src.main.matcher.tfidf_matcher import TfIdfMatch
from eventseries.src.main.matcher.word2vec_matcher import Word2VecMatch
from eventseries.src.main.repository.completions import Match
from eventseries.src.main.repository.completions import get_titles_from_match
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import WikiDataEvent, WikiDataEventSeries
from eventseries.src.main.util.fetch_miscellaneous_event_series import FetchEventSeries
from eventseries.src.main.util.record_attributes import LABEL, SERIES, TYPE


def create_training_test_dataset(previous_found_matches: List[Match]) -> pd.DataFrame:
    match_titles = [get_titles_from_match(match) for match in previous_found_matches]
    list_of_dicts = [{"event": event, "series": series} for event, series in match_titles]
    # Convert the dictionary to a DataFrame
    return pd.DataFrame(list_of_dicts, columns=["event", "series"])


class NlpMatcher:
    """We use the DBLP matched events with event series
    to test our algorithms and then apply them to wikidata events"""

    def __init__(self, repository: Repository) -> None:
        # Dataframe with "event" and "series" as column containing titles of found matches.
        self.train_test_set: pd.DataFrame = create_training_test_dataset(repository.get_matches())

    def match(self, events: List[WikiDataEvent], event_series: List[WikiDataEventSeries]):

        phrase_matcher = PhraseMatch(self.train_test_set)
        phrase_matcher.fit()
        phrase_matches_df = phrase_matcher.wikidata_match(events, event_series)
        phrase_matches_df[TYPE] = "Phrase_matching"
        events_df = (
            events_df.merge(phrase_matches_df, on="event_id", how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns=["_merge", "event_title", "series_title", "series_id"])
        )
        events_df.reset_index(drop=True, inplace=True)

        acronym_matcher = AcronymMatch(self.train_test_set)
        acronym_matcher.matcher()
        acronym_matches_df = acronym_matcher.wikidata_match(events_df, event_series_df)
        acronym_matches_df[TYPE] = "Acronym_matching"
        events_df = (
            events_df.merge(acronym_matches_df, on="event_id", how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns=["_merge", "event_title", "series_title", "series_id"])
        )
        events_df.reset_index(drop=True, inplace=True)

        ngram_matcher = NgramMatch(self.train_test_set)
        ngram_matcher.fit()
        n_gram_matches_df = ngram_matcher.match_events_to_series(events_df, event_series_df)
        events_df = (
            events_df.merge(n_gram_matches_df, on="event_id", how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns=["_merge", "event_title", "series_title", "series_id"])
        )
        events_df.reset_index(drop=True, inplace=True)
        n_gram_matches_df[TYPE] = "Ngram_matching"

        tf_idf_matcher = TfIdfMatch(self.train_test_set)
        tf_idf_matcher.matcher()
        tf_idf_matches_df = tf_idf_matcher.wikidata_match(events_df, event_series_df)
        events_df = (
            events_df.merge(tf_idf_matches_df, on="event_id", how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns=["_merge", "event_title", "series_title", "series_id"])
        )
        events_df.reset_index(drop=True, inplace=True)
        tf_idf_matches_df[TYPE] = "TfIdf_matching"

        naive_word2vec_matcher = NaiveWord2VecMatch(self.train_test_set)
        naive_word2vec_matcher.matcher()
        naive_word2vec_matches_df = naive_word2vec_matcher.wikidata_match(
            events_df, event_series_df
        )
        if len(naive_word2vec_matches_df) != 0:
            events_df = (
                events_df.merge(
                    naive_word2vec_matches_df, on="event_id", how="left", indicator=True
                )
                .query('_merge == "left_only"')
                .drop(columns=["_merge", "event_title", "series_title", "series_id"])
            )
            events_df.reset_index(drop=True, inplace=True)
        naive_word2vec_matches_df[TYPE] = "Naive_word2Vec_matching"

        # Since our training data is less we start with skip grams = 0 i.e. - CBOW
        word2vec_matcher_sg_0 = Word2VecMatch(self.train_test_set, 0)
        word2vec_matcher_sg_0.matcher()
        word2vec_matches_sg_0_matches_df = word2vec_matcher_sg_0.wikidata_match(
            events_df, event_series_df
        )
        events_df = (
            events_df.merge(
                word2vec_matches_sg_0_matches_df, on="event_id", how="left", indicator=True
            )
            .query('_merge == "left_only"')
            .drop(columns=["_merge", "event_title", "series_title", "series_id"])
        )
        events_df.reset_index(drop=True, inplace=True)
        word2vec_matches_sg_0_matches_df[TYPE] = "CBOW_word2vec_matching"

        word2vec_matcher_sg_1 = Word2VecMatch(self.train_test_set, 1)
        word2vec_matcher_sg_1.matcher()
        word2vec_matches_sg_1_matches_df = word2vec_matcher_sg_1.wikidata_match(
            events_df, event_series_df
        )
        events_df = (
            events_df.merge(
                word2vec_matches_sg_1_matches_df, on="event_id", how="left", indicator=True
            )
            .query('_merge == "left_only"')
            .drop(columns=["_merge", "event_title", "series_title", "series_id"])
        )
        events_df.reset_index(drop=True, inplace=True)
        word2vec_matches_sg_1_matches_df[TYPE] = "Skip_gram_word2vec_matching"

        results_df = pd.concat(
            [
                phrase_matches_df,
                acronym_matches_df,
                n_gram_matches_df,
                tf_idf_matches_df,
                naive_word2vec_matches_df,
                word2vec_matches_sg_0_matches_df,
                word2vec_matches_sg_1_matches_df,
            ],
            ignore_index=True,
        )

        return results_df

    """We create a training and test dataset out of the matches from:
    DBLP (Matches from event to event series for conferences)
    Existing matches from Wikidata
    Full matches from wikidata titles/labels/CEUR-WS event titles with event series titles"""

    def extract_series(self, wikidata_events_with_series):
        matches = []
        series_file = os.path.join(os.path.abspath("../main/resources"), "event_series.json")

        with open(series_file) as file:
            series = json.load(file)

        for item in wikidata_events_with_series:
            for series_item in series["results"]["bindings"]:
                if item[SERIES] == series_item["series"]["value"]:
                    if "seriesLabel" in series_item:
                        matches.append(Match(item[LABEL], series_item["seriesLabel"]["value"]))
        return matches

    def read_miscellaneous_event_series(self) -> List[str]:
        fetch_event_series = FetchEventSeries()
        return fetch_event_series.get_bard_event_series()
