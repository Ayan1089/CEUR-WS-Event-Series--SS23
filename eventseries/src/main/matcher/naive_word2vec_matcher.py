import re
from typing import List

import nltk
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from nltk.corpus import stopwords
from sklearn.metrics.pairwise import cosine_similarity


class NaiveWord2VecMatch:
    def __init__(self, matches_df: pd.DataFrame) -> None:
        matches_df.dropna(inplace=True)
        self.matches_df = matches_df
        self.matches_df.dropna(inplace=True)
        self.matches_df.reset_index(drop=True, inplace=True)
        self.stop_words = set(stopwords.words("english"))
        self.event_titles = self.remove_stopwords_tokenize(matches_df["event"].tolist())
        self.event_titles = [[token.lower() for token in tokens] for tokens in self.event_titles]
        self.series_titles = self.remove_stopwords_tokenize(matches_df["event_series"].tolist())
        self.series_titles = [[token.lower() for token in tokens] for tokens in self.series_titles]
        self.model = Word2Vec(self.event_titles + self.series_titles, vector_size=100, window=5, min_count=1, sg=0)
        self.matches_df["event_tokenized"] = self.event_titles
        self.matches_df["series_tokenized"] = self.series_titles
        self.recall = 0
        self.event_vector_list = []
        for event_tokens in self.event_titles:
            event_vector = np.mean([self.model.wv[token] for token in event_tokens], axis=0)
            event_vector = event_vector.reshape(1, -1)
            self.event_vector_list.append(event_vector)
        self.matches_df["event_vectors"] = self.event_vector_list

        series_vector_list = []
        for series_tokens in self.series_titles:
            series_vector = np.mean([self.model.wv[token] for token in series_tokens], axis=0)
            series_vector = series_vector.reshape(1, -1)
            series_vector_list.append(series_vector)
        self.matches_df["event_series_vectors"] = series_vector_list

    def matcher(self):
        true_positives = 0
        false_positives = 0
        for i in range(0, len(self.matches_df["event_vectors"])):
            max_similarity = 0
            best_j = -1
            for j in range(0, len(self.matches_df["event_series_vectors"])):
                similarity = cosine_similarity(self.matches_df.loc[i, "event_vectors"],
                                               self.matches_df.loc[j, "event_series_vectors"])[0][0]
                if i == j:
                    same_index_similarity = similarity
                if similarity > max_similarity:
                    max_similarity = similarity
                    best_j = j
            if i != best_j and max_similarity > same_index_similarity:
                false_positives += 1
                # print("EVENT: ", self.matches_df.loc[i, "event"])
                # print("SERIES: ", self.matches_df.loc[best_j, "event_series"])
                # print("i: ", i)
                # print("j: ", best_j)
                # print()
            else:
                true_positives += 1

        # We consider all the events that did not give out a match as the false negative set.
        false_negatives = len(self.event_titles) - (true_positives + false_positives)

        print(f"\nStatistics from Naive Word2Vec matching: ")
        precision = true_positives / (true_positives + false_positives)
        print("Precision: ", precision)
        recall = true_positives / (true_positives + false_negatives)
        print("Recall: ", recall)
        self.recall = recall
        f1_score = 2 * (precision * recall) / (precision + recall)
        print("F1-Score: ", f1_score)

    def wikidata_match(
            self,
            events_df: pd.DataFrame,
            series_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if self.recall == 1:
            print("Model is overfitting, and cannot be used")
            return pd.DataFrame()
        event_tokenized_titles = self.remove_stopwords_tokenize(events_df["title"].tolist())
        event_tokenized_titles = [[token.lower() for token in tokens] for tokens in event_tokenized_titles]

        series_tokenized_titles = self.remove_stopwords_tokenize(series_df["title"].tolist())
        series_tokenized_titles = [[token.lower() for token in tokens] for tokens in series_tokenized_titles]

        data_list_single_column = [[inner_list] for inner_list in event_tokenized_titles]
        matches_events_df = pd.DataFrame(
            {"events_tokenized": data_list_single_column, "events": events_df["title"].tolist(),
             "event_id": events_df["event_id"].tolist()})

        data_list_single_column = [[inner_list] for inner_list in series_tokenized_titles]
        matches_series_df = pd.DataFrame(
            {"series_tokenized": data_list_single_column, "event_series": series_df["title"].tolist(),
             "series_id": series_df["series_id"].tolist()})

        event_vector_list = []
        for event_tokens in event_tokenized_titles:
            event_vector = np.mean([self.model.wv[token] for token in event_tokens if token in self.model.wv], axis=0)
            event_vector = event_vector.reshape(1, -1)
            event_vector_list.append(event_vector)
        matches_events_df["event_vectors"] = event_vector_list

        series_vector_list = []
        for series_tokens in series_tokenized_titles:
            series_vector = np.mean([self.model.wv[token] for token in series_tokens if token in self.model.wv], axis=0)
            series_vector = series_vector.reshape(1, -1)
            series_vector_list.append(series_vector)
        matches_series_df["event_series_vectors"] = series_vector_list

        matching_events = []
        matching_events_ids = []
        matching_series = []
        matching_series_ids = []
        for i in range(0, len(matches_events_df["event_vectors"])):
            max_similarity = 0
            best_event = None
            best_event_id = None
            best_series = None
            best_series_id = None
            for j in range(0, len(matches_series_df["event_series_vectors"])):
                try:
                    similarity = cosine_similarity(matches_events_df.loc[i, "event_vectors"],
                                                   matches_series_df.loc[j, "event_series_vectors"])[0][0]
                except ValueError:
                    continue
                if similarity > max_similarity:
                    best_event = matches_events_df.loc[i, "events_tokenized"]
                    best_event_id = matches_events_df.loc[i, "event_id"]
                    best_series = matches_series_df.loc[j, "series_tokenized"]
                    best_series_id = matches_series_df.loc[j, "series_id"]
                    max_similarity = similarity
            matching_events.append(best_event)
            matching_events_ids.append(best_event_id)
            matching_series.append(best_series)
            matching_series_ids.append(best_series_id)
        results_df = pd.DataFrame(
            {"event_title": matching_events, "event_id": matching_events_ids,
             "series_title": matching_series,
             "series_id": matching_series_ids})
        print("Number of Word2Vec matches from event titles in Wikidata: ", len(matching_events))
        return results_df

    def remove_stopwords_tokenize(self, text_list) -> List[List[str]]:
        # Remove stopwords from each string in the list
        filtered_list = []
        for text in text_list:
            # Keep only alphanumeric characters and spaces
            text = self.clean_string(text)
            # Tokenize the string into individual words
            words = nltk.word_tokenize(text)
            # Remove stopwords from the list of words
            filtered_words = [
                word for word in words if word.lower() not in self.stop_words
            ]
            filtered_list.append(filtered_words)
        return filtered_list

    def clean_string(self, title):
        # Remove brackets and special characters using regular expressions
        cleaned_title = re.sub(r'[^\w\s]', '', title)  # Keep only alphanumeric characters and spaces
        return cleaned_title
