import logging
from typing import List, Optional, Set, Dict

import pandas as pd
from nltk import ngrams

from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventSeries,
    get_title_else_label,
)


def dice_coefficient(ngrams_first: Set, ngrams_second: Set):
    if len(ngrams_first) == 0 and len(ngrams_second) == 0:
        return 0
    return (2 * len(ngrams_first.intersection(ngrams_second))) / (
        len(ngrams_first) + len(ngrams_second)
    )


class NgramMatch:
    def __init__(self, matches_df: pd.DataFrame) -> None:
        matches_df.dropna(inplace=True)
        self.matches_df = matches_df
        self.event_titles_to_series_titles: Dict[str, str] = {}
        for _, row in matches_df.iterrows():
            self.event_titles_to_series_titles[row["event"]] = row["series"]
        self.n_grams = [3, 4, 5]
        self.recall = 0.835909631391201
        self.threshold_values = [0.8, 0.7, 0.6]
        self.best_threshold = 0.6
        self.best_n = 3
        self.fit()

    def fit(self):
        max_f1_score = 0
        # max_matches = 0
        best_precision = 0
        best_recall = 0
        best_n_gram = 0
        best_threshold = 0

        all_series_titles: List[str] = list(self.event_titles_to_series_titles.values())

        for n_gram_size in self.n_grams:
            for threshold in self.threshold_values:
                # threshold is the minimum required similarity for a partial match.
                true_positives = 0
                false_positives = 0
                false_negatives = 0
                for event, true_match in self.event_titles_to_series_titles.items():
                    matched_series: Optional[str] = self.match_to_series(
                        event, all_series_titles, n_gram_size, threshold
                    )

                    if matched_series is not None and matched_series == true_match:
                        true_positives += 1
                    elif matched_series is None:
                        # We consider all events that did not match as false negative set.
                        false_negatives += 1
                    else:
                        # We found a match, but it wasn't the right one.
                        false_positives += 1

                # Calculate the quality of matches.
                precision = true_positives / (true_positives + false_positives)
                recall = true_positives / (true_positives + false_negatives)
                f1_score = 2 * (precision * recall) / (precision + recall)

                # Log results for this combination of parameter.
                logging.debug(
                    "Statistics for %s n-grams and threshold: %s ->", n_gram_size, threshold
                )
                logging.debug("true positives: %s", true_positives)
                logging.debug("false positives: %s", false_positives)
                logging.debug("false negatives: %s", false_negatives)
                logging.debug("Precision %s, recall %s and f1 %s", precision, recall, f1_score)

                if f1_score > max_f1_score:
                    best_precision = precision
                    best_recall = recall
                    max_f1_score = f1_score
                    best_n_gram = n_gram_size
                    best_threshold = threshold

        # After all combinations of threshold and n_gram_size were tested.
        logging.debug("Best Choice for n-grams: ")
        logging.debug("Statistics for %s n-grams and threshold: %s ->", best_n_gram, best_threshold)
        logging.debug("Precision: %s", best_precision)
        logging.debug("Recall: %s", best_recall)
        self.recall = best_recall
        logging.debug("F1-Score: %s", max_f1_score)
        self.best_n = best_n_gram
        self.best_threshold = best_threshold

    @staticmethod
    def match_to_series(
        event: str, series_list: List[str], n_gram_size, threshold, word_wise=False
    ):
        event_ngrams = set(ngrams(event.split() if word_wise else event, n_gram_size))
        best_similarity_value: Optional[float] = None
        best_match: Optional[str] = None
        for series in series_list:
            series_ngrams = set(ngrams(series.split() if word_wise else series, n_gram_size))
            similarity = dice_coefficient(event_ngrams, series_ngrams)
            if similarity < threshold:
                continue
            if best_similarity_value is None or similarity > best_similarity_value:
                best_match = series
                best_similarity_value = similarity

        return best_match

    def match_events_to_series(
        self, event_list: List[WikiDataEvent], series_list: List[WikiDataEventSeries]
    ) -> List[FullMatch]:
        if self.recall == 1:
            logging.error("Model is overfitting, and cannot be used")
            return []
        series_title_to_series = {get_title_else_label(series): series for series in series_list}

        found_matches: List[FullMatch] = []
        for event in event_list:
            event_title = get_title_else_label(event)
            found_match = NgramMatch.match_to_series(
                event_title, list(series_title_to_series.keys()), self.best_n, self.best_threshold
            )
            if found_match is None:
                continue
            found_matches.append(
                FullMatch(
                    event=event,
                    series=series_title_to_series[found_match],
                    found_by="NgramMatch::wikidata_match",
                )
            )

        return found_matches
