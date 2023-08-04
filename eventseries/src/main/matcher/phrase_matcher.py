import json
import os

import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher


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
        series_titles = matches_df["event_series"].tolist()
        patterns = [self.nlp.make_doc(text) for text in series_titles]
        self.event_titles = matches_df["event"].tolist()
        self.phrase_matcher.add("Event_EventSeries_Matcher", patterns)
        # Capturing all the distinct series
        self.series_distinct = []

    def matcher(self):
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
                    self.matches_df.loc[
                        self.matches_df["event"] == event, "event_series"
                    ].values[0]
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
        print("Statistics from DBLP: ")
        precision = true_positives / (true_positives + false_positives)
        print("Precision: ", precision)
        recall = true_positives / (true_positives + false_negatives)
        print("Recall: ", recall)
        f1_score = 2 * (precision * recall) / (precision + recall)
        print("F1-Score: ", f1_score)

        # print("Number of containment matches from event titles: ", len(matching_events))

    def wikidata_match(self):
        events_file = os.path.join(
            os.path.abspath("resources"), "events_without_matches.json"
        )
        series_file = os.path.join(os.path.abspath("resources"), "event_series.json")
        with open(events_file) as file:
            events = json.load(file)
            event_titles = [item["title"] for item in events if "title" in item]
        with open(series_file) as file:
            series = json.load(file)
            series_titles = [
                item["title"]["value"]
                for item in series["results"]["bindings"]
                if "title" in item
            ]

        nlp = spacy.load("en_core_web_sm")
        patterns = [nlp.make_doc(text) for text in series_titles]
        phrase_matcher = spacy.matcher.PhraseMatcher(nlp.vocab)
        phrase_matcher.add("Event_EventSeries_Matcher", patterns)

        matching_events = []
        series_distinct = []
        for event in event_titles:
            doc = nlp(event)
            matches = phrase_matcher(doc)
            for match_id, start, end in matches:
                span = doc[start:end]
                if event not in matching_events:
                    matching_events.append(event)
                if span.text not in series_distinct:
                    series_distinct.append(span.text)
        #         print(f"Series: '{span.text}' Event: '{event}'")
        print(
            "Number of containment matches from event titles in Wikidata: ",
            len(matching_events),
        )
        return matching_events
