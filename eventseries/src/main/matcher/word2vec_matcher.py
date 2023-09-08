import logging
import re
from typing import List

import gensim.downloader as api
import nltk
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from nltk.corpus import stopwords
from sklearn.metrics.pairwise import cosine_similarity

from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventSeries,
    get_title_else_label,
)


class Word2VecMatch:
    def __init__(self, matches_df: pd.DataFrame, skip_grams: int) -> None:
        matches_df.dropna(inplace=True)
        self.matches_df = matches_df
        self.matches_df.dropna(inplace=True)
        self.matches_df.reset_index(drop=True, inplace=True)
        self.stop_words = set(stopwords.words("english"))
        self.event_titles = self.remove_stopwords_tokenize(matches_df["event"].tolist())
        self.event_titles = [[token.lower() for token in tokens] for tokens in self.event_titles]
        self.series_titles = self.remove_stopwords_tokenize(matches_df["series"].tolist())
        self.series_titles = [[token.lower() for token in tokens] for tokens in self.series_titles]
        self.skip_grams = skip_grams
        glove_model = api.load("glove-wiki-gigaword-300")
        self.model = Word2Vec(
            self.event_titles + self.series_titles,
            vector_size=100,
            window=5,
            min_count=1,
            sg=self.skip_grams,
        )
        self.model.wv.vectors = glove_model.vectors
        self.matches_df["event_tokenized"] = self.event_titles
        self.matches_df["series_tokenized"] = self.series_titles
        self.recall = 0
        self.event_vector_list = []
        for event_tokens in self.event_titles:
            event_vector = np.mean([self.model.wv[token] for token in event_tokens], axis=0)
            event_vector = event_vector.reshape(1, -1)
            self.event_vector_list.append(event_vector)
        self.matches_df["event_vectors"] = self.event_vector_list
        self.best_threshold = 0

        series_vector_list = []
        for series_tokens in self.series_titles:
            series_vector = np.mean([self.model.wv[token] for token in series_tokens], axis=0)
            series_vector = series_vector.reshape(1, -1)
            series_vector_list.append(series_vector)
        self.matches_df["event_series_vectors"] = series_vector_list
        self.matcher()

    def matcher(self):
        true_positives = 0
        false_positives = 0
        false_negatives = 0
        similarity_threshold = [0.93, 0.95]
        best_f1score = 0

        for x in range(0, len(similarity_threshold)):
            for i in range(0, len(self.matches_df["event_vectors"])):
                max_similarity = 0
                best_j = -1
                for j in range(0, len(self.matches_df["event_series_vectors"])):
                    similarity = cosine_similarity(
                        self.matches_df.loc[i, "event_vectors"],
                        self.matches_df.loc[j, "event_series_vectors"],
                    )[0][0]
                    if i == j:
                        same_index_similarity = similarity
                    if similarity > max_similarity:
                        max_similarity = similarity
                        best_j = j
                if max_similarity < similarity_threshold[x]:
                    false_negatives += 1
                elif i != best_j and max_similarity > same_index_similarity:
                    false_positives += 1
                else:
                    true_positives += 1

            precision = true_positives / (true_positives + false_positives)
            recall = true_positives / (true_positives + false_negatives)
            self.recall = recall
            f1_score = 2 * (precision * recall) / (precision + recall)
            if f1_score > best_f1score:
                best_f1score = f1_score
                self.best_threshold = similarity_threshold[x]

            # print(f"\nStatistics from Word2Vec matching with skip grams: ", self.skip_grams)
            # print("Precision: ", precision)
            # # recall = true_positives / (true_positives + false_negatives)
            # print("Recall: ", recall)
            # self.recall = recall
            # # f1_score = 2 * (precision * recall) / (precision + recall)
            # print("F1-Score: ", f1_score)

        if self.skip_grams == 0:
            logging.info("Statistics from Word2Vec matching for continuous bag of words: ")
        else:
            logging.info("Statistics from Word2Vec matching with skip grams: %s", self.skip_grams)
        logging.info("Best threshold: %s", self.best_threshold)
        precision = true_positives / (true_positives + false_positives)
        logging.info("Precision: %s", precision)
        recall = true_positives / (true_positives + false_negatives)
        logging.info("Recall: %s", recall)
        self.recall = recall
        f1_score = 2 * (precision * recall) / (precision + recall)
        logging.info("F1-Score: %s", f1_score)

    def wikidata_match(
        self, events_list: List[WikiDataEvent], series_list: List[WikiDataEventSeries]
    ) -> List[FullMatch]:
        if self.recall == 1:
            logging.error("Model is overfitting, and cannot be used")
            return []

        event_titles = [get_title_else_label(event) for event in events_list]
        series_titles = [get_title_else_label(series) for series in series_list]

        event_tokenized_titles = self.remove_stopwords_tokenize(event_titles)
        event_tokenized_titles = [
            [token.lower() for token in tokens] for tokens in event_tokenized_titles
        ]

        series_tokenized_titles = self.remove_stopwords_tokenize(series_titles)
        series_tokenized_titles = [
            [token.lower() for token in tokens] for tokens in series_tokenized_titles
        ]

        data_list_single_column = [[inner_list] for inner_list in event_tokenized_titles]
        matches_events_df = pd.DataFrame(
            {
                "events_tokenized": data_list_single_column,
                "title": event_titles,
                "event": events_list,
            }
        )

        data_list_single_column = [[inner_list] for inner_list in series_tokenized_titles]
        matches_series_df = pd.DataFrame(
            {
                "series_tokenized": data_list_single_column,
                "title": series_titles,
                "series": series_list,
            }
        )

        event_vector_list = []
        for event_tokens in event_tokenized_titles:
            event_vector = np.mean(
                [self.model.wv[token] for token in event_tokens if token in self.model.wv], axis=0
            )
            event_vector = event_vector.reshape(1, -1)
            event_vector_list.append(event_vector)
        matches_events_df["event_vectors"] = event_vector_list

        series_vector_list = []
        for series_tokens in series_tokenized_titles:
            series_vector = np.mean(
                [self.model.wv[token] for token in series_tokens if token in self.model.wv], axis=0
            )
            series_vector = series_vector.reshape(1, -1)
            series_vector_list.append(series_vector)
        matches_series_df["event_series_vectors"] = series_vector_list

        found_matches: List[FullMatch] = []
        for i in range(0, len(matches_events_df["event_vectors"])):
            similarities = {}
            for j in range(0, len(matches_series_df["event_series_vectors"])):
                try:
                    similarity = cosine_similarity(
                        matches_events_df.loc[i, "event_vectors"],
                        matches_series_df.loc[j, "event_series_vectors"],
                    )[0][0]
                    similarities[similarity] = j
                except ValueError:
                    continue

            if len(similarities.keys()) != 0 and max(similarities.keys()) > self.best_threshold:
                best_event_id = matches_events_df.loc[i, "event"]
                best_series_id = matches_series_df.loc[
                    similarities[max(similarities.keys())], "series"
                ]
                found_matches.append(
                    FullMatch(
                        event=best_event_id,
                        series=best_series_id,
                        found_by="Word2VecMatch::wikidata_match",
                    )
                )
        return found_matches

    def remove_stopwords_tokenize(self, text_list) -> List[List[str]]:
        # Remove stopwords from each string in the list
        filtered_list = []
        for text in text_list:
            # Keep only alphanumeric characters and spaces
            text = self.clean_string(text)
            # Tokenize the string into individual words
            words = nltk.word_tokenize(text)
            # Remove stopwords from the list of words
            filtered_words = [word for word in words if word.lower() not in self.stop_words]
            filtered_list.append(filtered_words)
        return filtered_list

    def clean_string(self, title):
        # Remove brackets and special characters using regular expressions
        cleaned_title = re.sub(
            r"[^\w\s]", "", title
        )  # Keep only alphanumeric characters and spaces
        return cleaned_title
