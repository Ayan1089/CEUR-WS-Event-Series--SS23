from typing import Dict, List

import nltk
import pandas as pd
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.metrics.distance import jaccard_distance


class NgramMatch:
    def __init__(self, matches_df: pd.DataFrame) -> None:
        matches_df.dropna(inplace=True)
        self.matches_df = matches_df
        self.stop_words = set(stopwords.words("english"))
        self.event_titles = self.remove_stopwords(matches_df["event"].tolist())
        self.matches_df["event"] = self.event_titles
        self.series_titles = self.remove_stopwords(matches_df["event_series"].tolist())
        self.matches_df["event_series"] = self.series_titles
        self.series_distinct: List[str] = []
        self.n_grams = [3, 4, 5]
        self.recall = 0
        self.threshold_values = [1, 0.9, 0.8, 0.7, 0.6, 0.5]
        self.best_threshold = 0
        self.best_n = 0

    def matcher(self):
        max_f1_score = 0
        # max_matches = 0
        best_precision = 0
        best_recall = 0
        best_n_gram = 0
        best_threshold = 0

        for i in self.n_grams:
            n = i  # Number of words in each n-gram
            for j in self.threshold_values:
                threshold = j  # Minimum required similarity for a partial match
                true_positives = 0
                false_positives = 0
                false_negatives = 0
                partially_matched_events = []
                for event in self.event_titles:
                    # We need this dict because there can be a many-to-many mapping between event and event series
                    matched_events_dict: Dict[str, float] = {}
                    matched_series = ""
                    event_ngrams = set(ngrams(event.split(), n))
                    for series in self.series_titles:
                        series_ngrams = set(ngrams(series.split(), n))
                        """There can be cases that series or events don't have 3 words"""
                        similarity = 0
                        if len(event_ngrams.union(series_ngrams)) > 0:
                            similarity = 1 - jaccard_distance(
                                event_ngrams, series_ngrams
                            )

                        if (
                            (similarity >= threshold)
                            and (
                                event in matched_events_dict
                                and similarity > matched_events_dict[event]
                            )
                        ) or (
                            event not in matched_events_dict and similarity >= threshold
                        ):
                            matched_events_dict[event] = similarity
                            matched_series = series

                    #             print("Partial match found:")
                    #             print(f"#####MATCHED_EVENT#####{event}")
                    #             print(f"######MATCHED_SERIES######{matched_series}")
                    #             print()
                    if (
                        self.matches_df.loc[
                            self.matches_df["event"] == event, "event_series"
                        ].values[0]
                    ) == matched_series:
                        true_positives += 1
                    elif matched_series == "":
                        # We consider all the events that did not give out a match as the false negative set.
                        false_negatives += 1
                    else:
                        false_positives += 1

                    if series not in self.series_distinct:
                        self.series_distinct.append(series)
                    #             print()
                    partially_matched_events.append(event)

                # print(f"Statistics for {n}-grams and threshold:{threshold}->")
                # print()
                # print("true positives: ", true_positives)
                # print("false positives: ", false_positives)
                # print("false negatives: ", false_negatives)
                precision = true_positives / (true_positives + false_positives)
                # print("Precision: ", precision)
                recall = true_positives / (true_positives + false_negatives)
                # print("Recall: ", recall)
                f1_score = 2 * (precision * recall) / (precision + recall)
                if f1_score > max_f1_score:
                    best_precision = precision
                    best_recall = recall
                    max_f1_score = f1_score
                    # max_matches = len(partially_matched_events)
                    best_n_gram = n
                    best_threshold = threshold
                # print("F1-Score: ", f1_score)
                # print("Number of partial matches: ", len(partially_matched_events))
                # print()

        print("Best Choice for n-grams: ")
        print(f"Statistics for {best_n_gram}-grams and threshold: {best_threshold}->")
        print("Precision: ", best_precision)
        print("Recall: ", best_recall)
        self.recall = best_recall
        print("F1-Score: ", max_f1_score)
        # print("Maximum number of partial matches: ", max_matches)
        self.best_n = best_n_gram
        self.best_threshold = best_threshold

    def wikidata_match(
        self,
        events_df: pd.DataFrame,
        series_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if self.recall == 1:
            print("Model is overfitting, and cannot be used")
            return pd.DataFrame()
        partially_matched_events = []
        partially_matched_events_ids = []
        partially_matched_series = []
        partially_matched_series_ids = []

        for i in range(0, len(events_df)):
            matched_events_dict: Dict[str, float] = {}
            matched_series = ""
            matched_series_id = ""
            event_ngrams = set(ngrams(events_df.loc[i, "title"].split(), self.best_n))
            for j in range(0, len(series_df)):
                series_ngrams = set(ngrams(series_df.loc[j, "title"].split(), self.best_n))
                """There can be cases that series or events don't have 3 words"""
                if len(event_ngrams.union(series_ngrams)) > 0:
                    similarity = 1 - jaccard_distance(event_ngrams, series_ngrams)

                if (
                    (similarity >= self.best_threshold)
                    and (
                        events_df.loc[i, "title"] in matched_events_dict
                        and similarity > matched_events_dict[events_df.loc[i, "title"]]
                    )
                ) or (
                    events_df.loc[i, "title"] not in matched_events_dict
                    and similarity >= self.best_threshold
                ):
                    matched_events_dict[events_df.loc[i, "title"]] = similarity
                    matched_series = series_df.loc[j, "title"]
                    matched_series_id = series_df.loc[j, "series_id"]
            # print("Partial match found:")
            # print(f"#####EVENT#####{event}")
            # print(f"######SERIES######{matched_series}")
            #     if series not in series_distinct:
            #         series_distinct.append(series)
            # print()
            if matched_series != "":
                partially_matched_events.append(events_df.loc[i, "title"])
                partially_matched_events_ids.append(events_df.loc[i, "event_id"])
                partially_matched_series.append(matched_series)
                partially_matched_series_ids.append(matched_series_id)


        print(
            "Number of unique matches from n-grams in Wikidata: ",
            len(partially_matched_events),
        )
        results_df = pd.DataFrame(
            {"event_title": partially_matched_events, "event_id": partially_matched_events_ids, "series_title": partially_matched_series,
             "series_id": partially_matched_series_ids})

        return results_df

    def remove_stopwords(self, text_list):
        # Remove stopwords from each string in the list
        filtered_list = []
        for text in text_list:
            # Tokenize the string into individual words
            words = nltk.word_tokenize(text)
            # Remove stopwords from the list of words
            filtered_words = [
                word for word in words if word.lower() not in self.stop_words
            ]
            # Join the filtered words back into a string
            filtered_text = " ".join(filtered_words)
            # Add the filtered string to the filtered list
            filtered_list.append(filtered_text)
        return filtered_list
