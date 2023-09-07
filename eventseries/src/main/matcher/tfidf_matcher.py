import logging
from typing import List

import numpy as np
import pandas as pd
from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventSeries,
    get_title_else_label,
)


class TfIdfMatch:
    def __init__(self, matches_df: pd.DataFrame) -> None:
        self.matches_df = matches_df
        self.matches_df.dropna(inplace=True)
        self.series_titles = matches_df["series"].tolist()
        self.event_titles = matches_df["event"].tolist()
        self.best_threshold = -1
        self.best_f1_score = -1
        self.recall = 0
        self.fit()

    def fit(self):
        vectorizer = TfidfVectorizer(stop_words=list(text.ENGLISH_STOP_WORDS))
        array1_strings = self.event_titles
        array2_strings = self.series_titles
        tfidf_matrix = vectorizer.fit_transform(self.event_titles + self.series_titles)

        # Calculate cosine similarity between array1 and array2
        similarity_matrix = cosine_similarity(
            tfidf_matrix[: len(array1_strings)], tfidf_matrix[len(array1_strings) :]
        )

        # Threshold for partial match
        threshold_values = [0.5, 0.6, 0.7, 0.8, 0.9]
        # threshold_values = [0.9]

        # Find partial matches\
        for threshold in threshold_values:
            matches = []
            true_positives = 0
            false_positives = 0
            false_negatives = 0
            num_rows = len(similarity_matrix)
            num_cols = len(similarity_matrix[0])
            for row in range(num_rows):
                score = threshold
                new_row = -1
                new_col = -1
                for col in range(num_cols):
                    element = similarity_matrix[row][col]
                    if element >= score:
                        score = element
                        new_row = row
                        new_col = col
                if new_row != -1 and new_col != -1:
                    matches.append([new_row, new_col])
            #     print(matches)

            # matches = np.argwhere(similarity_matrix >= threshold)

            ctr = 0
            partially_matched_events = []

            # Print partial matches
            for match in matches:
                array1_index = match[0]
                array2_index = match[1]
                #         print("Partial match found:")
                #         print(f"#####EVENT#####{array1_strings[array1_index]}")
                #         print(f"######SERIES######{array2_strings[array2_index]}")

                if (
                    self.matches_df.loc[
                        self.matches_df["event"] == array1_strings[array1_index], "series"
                    ].values[0]
                ) == array2_strings[array2_index]:
                    #         if(matched_events_dict[array1_strings[array1_index]] >)
                    true_positives += 1
                else:
                    false_positives += 1
                partially_matched_events.append(array1_strings[array1_index])

            # Series not matched to any event
            #     false_negatives = len(series_titles) - len(series_distinct)
            false_negatives = len(self.event_titles) - (true_positives + false_positives)

            # print("Results with threshold: ", threshold)
            # print("true positives: ", true_positives)
            # print("false positives: ", false_positives)
            # print("false negatives: ", false_negatives)
            precision = true_positives / (true_positives + false_positives)
            # print("Precision: ", precision)
            recall = true_positives / (true_positives + false_negatives)
            self.recall = recall
            # print("Recall: ", recall)
            f1_score = 2 * (precision * recall) / (precision + recall)
            if f1_score > self.best_f1_score:
                self.best_threshold = threshold
                self.best_f1_score = f1_score
        logging.info(
            "Best f1 score for TfIdfMatch is %s with threshold %s",
            self.best_f1_score,
            self.best_threshold,
        )

    def wikidata_match(
        self, events_list: List[WikiDataEvent], series_list: List[WikiDataEventSeries]
    ) -> List[FullMatch]:
        if self.recall == 1:
            print("Model is overfitting, and cannot be used")
            return []

        vectorizer = TfidfVectorizer(stop_words=list(text.ENGLISH_STOP_WORDS))
        event_titles = [get_title_else_label(event) for event in events_list]
        series_titles = [get_title_else_label(series) for series in series_list]
        tfidf_matrix = vectorizer.fit_transform(event_titles + series_titles)

        # Calculate cosine similarity between array1 and array2
        similarity_matrix = cosine_similarity(
            tfidf_matrix[: len(event_titles)], tfidf_matrix[len(event_titles) :]
        )

        # Threshold for partial match
        threshold = self.best_threshold

        # Find partial matches
        matches = np.argwhere(similarity_matrix >= threshold)
        found_full_matches = []
        # Print partial matches
        for match in matches:
            array1_index = match[0]
            array2_index = match[1]
            found_full_matches.append(
                FullMatch(
                    event=events_list[array1_index],
                    series=series_list[array2_index],
                    found_by="TfIdfMatch::wikidata_match",
                )
            )
        return found_full_matches
