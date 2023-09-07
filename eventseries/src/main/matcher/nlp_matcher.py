import json
import os
from pathlib import Path
from typing import List

import pandas as pd

from eventseries.src.main.dblp import matching
from eventseries.src.main.dblp.parsing import load_event_series
from eventseries.src.main.matcher.acronym_matcher import AcronymMatch
from eventseries.src.main.matcher.match import Match
from eventseries.src.main.matcher.naive_word2vec_matcher import NaiveWord2VecMatch
from eventseries.src.main.matcher.ngram_matcher import NgramMatch
from eventseries.src.main.matcher.phrase_matcher import PhraseMatch
from eventseries.src.main.matcher.tfidf_matcher import TfIdfMatch
from eventseries.src.main.matcher.wikidata_matcher import Matcher
from eventseries.src.main.matcher.word2vec_matcher import Word2VecMatch
from eventseries.src.main.parsers.event_extractor import EventExtractor
from eventseries.src.main.util.fetch_miscellaneous_event_series import FetchEventSeries
from eventseries.src.main.util.record_attributes import LABEL, SERIES, TITLE, TYPE
from eventseries.src.main.util.utility import Utility


class NlpMatcher:
    """We use the DBLP matched events with event series
    to test our algorithms and then apply them to wikidata events"""

    def __init__(self, event_extractor: EventExtractor, matcher: Matcher) -> None:
        self.utility = Utility()
        self.df = self.create_training_test_dataset(
            event_extractor=event_extractor, matcher=matcher
        )

    def match(self, events_df: pd.DataFrame, event_series_df: pd.DataFrame):
        events = events_df["title"].tolist()
        event_series = event_series_df["title"].tolist()

        """Extend the event series list by adding miscellaneous event series"""
        miscellaneous_event_series = self.read_miscellaneous_event_series()
        for series in miscellaneous_event_series:
            event_series_df.loc[len(event_series_df)] = [series, "Bard_Series", ""]

        phrase_matcher = PhraseMatch(self.df)
        phrase_matcher.matcher()
        phrase_matches_df = phrase_matcher.wikidata_match(events_df, event_series_df)
        phrase_matches_df[TYPE] = "Phrase_matching"
        events_df = events_df.merge(phrase_matches_df, on="event_id", how="left", indicator=True).query(
            '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
        events_df.reset_index(drop=True, inplace=True)

        acronym_matcher = AcronymMatch(self.df)
        acronym_matcher.matcher()
        acronym_matches_df = acronym_matcher.wikidata_match(events_df, event_series_df)
        acronym_matches_df[TYPE] = "Acronym_matching"
        events_df = events_df.merge(acronym_matches_df, on="event_id", how="left", indicator=True).query(
            '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
        events_df.reset_index(drop=True, inplace=True)

        ngram_matcher = NgramMatch(self.df)
        ngram_matcher.matcher()
        n_gram_matches_df = ngram_matcher.wikidata_match(events_df, event_series_df)
        events_df = events_df.merge(n_gram_matches_df, on="event_id", how="left", indicator=True).query(
            '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
        events_df.reset_index(drop=True, inplace=True)
        n_gram_matches_df[TYPE] = "Ngram_matching"

        tf_idf_matcher = TfIdfMatch(self.df)
        tf_idf_matcher.matcher()
        tf_idf_matches_df = tf_idf_matcher.wikidata_match(events_df, event_series_df)
        events_df = events_df.merge(tf_idf_matches_df, on="event_id", how="left", indicator=True).query(
            '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
        events_df.reset_index(drop=True, inplace=True)
        tf_idf_matches_df[TYPE] = "TfIdf_matching"

        naive_word2vec_matcher = NaiveWord2VecMatch(self.df)
        naive_word2vec_matcher.matcher()
        naive_word2vec_matches_df = naive_word2vec_matcher.wikidata_match(events_df, event_series_df)
        if len(naive_word2vec_matches_df) != 0:
            events_df = events_df.merge(naive_word2vec_matches_df, on="event_id", how="left", indicator=True).query(
                '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
            events_df.reset_index(drop=True, inplace=True)
        naive_word2vec_matches_df[TYPE] = "Naive_word2Vec_matching"

        # Since our training data is less we start with skip grams = 0 i.e. - CBOW
        word2vec_matcher_sg_0 = Word2VecMatch(self.df, 0)
        word2vec_matcher_sg_0.matcher()
        word2vec_matches_sg_0_matches_df = word2vec_matcher_sg_0.wikidata_match(events_df, event_series_df)
        events_df = events_df.merge(word2vec_matches_sg_0_matches_df, on="event_id", how="left", indicator=True).query(
            '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
        events_df.reset_index(drop=True, inplace=True)
        word2vec_matches_sg_0_matches_df[TYPE] = "CBOW_word2vec_matching"

        word2vec_matcher_sg_1 = Word2VecMatch(self.df, 1)
        word2vec_matcher_sg_1.matcher()
        word2vec_matches_sg_1_matches_df = word2vec_matcher_sg_1.wikidata_match(events_df, event_series_df)
        events_df = events_df.merge(word2vec_matches_sg_1_matches_df, on="event_id", how="left", indicator=True).query(
            '_merge == "left_only"').drop(columns=["_merge", "event_title", "series_title", "series_id"])
        events_df.reset_index(drop=True, inplace=True)
        word2vec_matches_sg_1_matches_df[TYPE] = "Skip_gram_word2vec_matching"

        results_df = pd.concat(
            [phrase_matches_df, acronym_matches_df, n_gram_matches_df, tf_idf_matches_df, naive_word2vec_matches_df,
             word2vec_matches_sg_0_matches_df, word2vec_matches_sg_1_matches_df], ignore_index=True)

        return results_df

    """We create a training and test dataset out of the matches from:
    DBLP (Matches from event to event series for conferences)
    Existing matches from Wikidata
    Full matches from wikidata titles/labels/CEUR-WS event titles with event series titles"""

    def create_training_test_dataset(
            self, event_extractor: EventExtractor, matcher: Matcher
    ):
        matches = []
        resources_path = Path(__file__).resolve().parent / ".." / "resources"
        path_to_wikidata_events = os.path.join(
            resources_path, "EventsWithoutSeries.json"
        )
        # path_to_wikidata_events = Path("") / ".." / "resources" / "EventsWithoutSeries.json"

        resources_path = Path(__file__).resolve().parent / ".." / "resources"
        path = os.path.join(resources_path, "dblp_event_series.pickle")

        dblp_matches_df = matching.match_wikidata_conference_to_series_dblp_id(
            pd.read_json(path_to_wikidata_events),
            load_event_series(Path(path)),
        )
        dblp_matches_dict = dblp_matches_df[[TITLE, SERIES]].reset_index().to_dict()
        for item in range(0, len(dblp_matches_df)):
            matches.append(
                Match(
                    dblp_matches_dict[TITLE][item], dblp_matches_dict[SERIES][item].name
                )
            )

        wikidata_events_with_series = event_extractor.get_existing_matched_events()
        wikidata_events_with_series = self.extract_series(wikidata_events_with_series)

        matches += wikidata_events_with_series

        for match in matcher.get_all_matches():
            matches.append(Match(match[TITLE], match[TITLE]))

        data_dict = {}
        events = []
        event_series = []
        for item in matches:
            events.append(item.get_event())
            event_series.append(item.get_event_series())
        data_dict["event"] = events
        data_dict["event_series"] = event_series

        # Convert the dictionary to a DataFrame
        df = pd.DataFrame(data_dict)
        df.to_json(
            "/Users/ayan/Projects/KGLab/main/CEUR-WS-Event-Series--SS23/eventseries/src/main/resources/all_matches.json"
        )
        return df

    def extract_series(self, wikidata_events_with_series):
        matches = []
        series_file = os.path.join(
            Path(__file__).resolve().parent / ".." / "resources", "event_series.json"
        )

        with open(series_file) as file:
            series = json.load(file)

        for item in wikidata_events_with_series:
            for series_item in series["results"]["bindings"]:
                if item[SERIES] == series_item["series"]["value"]:
                    if "seriesLabel" in series_item:
                        matches.append(
                            Match(item[LABEL], series_item["seriesLabel"]["value"])
                        )
        return matches

    def read_miscellaneous_event_series(self) -> List[str]:
        fetch_event_series = FetchEventSeries()
        return fetch_event_series.get_bard_event_series()
