import logging
from typing import List, Set

import pandas as pd
import spacy
import spacy.matcher

from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventSeries,
    get_title_else_label,
    QID,
)


# from typing import List


class PhraseMatch:
    """
    It has usually been observed that the event titles are larger than the series title
    Example -
    EVENT - 2nd International Semantic Web Conference (https://www.wikidata.org/wiki/Q48027371)
    EVENT_SERIES - International Semantic Web Conference (https://www.wikidata.org/wiki/Q6053150)
    """

    def __init__(self, matches_df: pd.DataFrame) -> None:
        self.nlp = spacy.load("en_core_web_sm")
        self.phrase_matcher = spacy.matcher.PhraseMatcher(self.nlp.vocab)
        # Only run nlp.make_doc to speed things up
        self.matches_df = matches_df
        self.matches_df.dropna(inplace=True)
        self.recall = 0
        series_titles = matches_df["series"].tolist()
        patterns = [self.nlp.make_doc(text) for text in series_titles]
        self.event_titles = matches_df["event"].tolist()
        self.phrase_matcher.add("Event_EventSeries_Matcher", patterns)
        # Capturing all the distinct series
        self.series_distinct: List[str] = []

    def test_accuracy(self):
        true_positives = 0
        false_positives = 0
        false_negatives = 0

        matching_events = []
        for event in self.event_titles:
            doc = self.nlp(event)
            matches = self.phrase_matcher(doc)
            for match_id, start, end in matches:
                span = doc[start:end]
                if event not in matching_events:
                    matching_events.append(event)
                if span.text not in self.series_distinct:
                    self.series_distinct.append(span.text)
                if (
                    self.matches_df.loc[self.matches_df["event"] == event, "series"].values[0]
                ) == span.text:
                    true_positives += 1
                else:
                    false_positives += 1
        #         print(f"Series: '{span.text}' Event: '{event}'")

        # We consider all the events that did not give out a match as the false negative set.
        false_negatives = len(self.event_titles) - (true_positives + false_positives)

        # print("true positives: ", true_positives)
        # print("false positives: ", false_positives)
        # print("false negatives: ", false_negatives)
        logging.info("Statistics from Phrase matching: ")
        precision = true_positives / (true_positives + false_positives)
        logging.info("Precision: %s", precision)
        recall = true_positives / (true_positives + false_negatives)
        self.recall = recall
        logging.info("Recall: %s", recall)
        f1_score = 2 * (precision * recall) / (precision + recall)
        logging.info("F1-Score: %s", f1_score)

        # print("Number of containment matches from event titles: ", len(matching_events))

    def wikidata_match(
        self, events: List[WikiDataEvent], event_series: List[WikiDataEventSeries]
    ) -> List[FullMatch]:
        if self.recall == 1:
            print("Model is overfitting, and cannot be used")
            return []
        series_titles_to_series = {get_title_else_label(series): series for series in event_series}

        nlp = spacy.load("en_core_web_sm")
        patterns = [nlp.make_doc(text) for text in series_titles_to_series.keys()]
        phrase_matcher = spacy.matcher.PhraseMatcher(nlp.vocab)
        phrase_matcher.add("Event_EventSeries_Matcher", patterns)

        found_matches = []
        matched_events: Set[QID] = set()
        for event in events:
            event_title = get_title_else_label(event)
            doc = nlp(event_title)
            matches = phrase_matcher(doc)
            for _, start, end in matches:
                if event.qid in matched_events:
                    break
                span = doc[start:end]
                series_from_title = series_titles_to_series.get(span.text)
                if series_from_title is None:
                    logging.error("Could not recreate series from span.text %s", span.text)
                    continue
                matched_events.add(event.qid)
                found_matches.append(
                    FullMatch(
                        event=event,
                        series=series_from_title,
                        found_by="PhraseMatch::wikidata_match",
                    )
                )

        return found_matches
