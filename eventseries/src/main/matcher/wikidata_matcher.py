import json
import os
import pickle


class Matcher:
    def __init__(self):
        self.event_series = []
        self.wikidata_matches = []

    def match(self, records_with_potential_attr, attr):
        titles = [record[attr] for record in records_with_potential_attr]
        self.event_series = self.get_event_series_title(
            os.path.join(Path(__file__).resolve().parent / ".." / "resources", "event_series.json")
        )
        matches = set(titles) & set(self.event_series)
        matching_records = []
        for match in matches:
            for record in records_with_potential_attr:
                if attr in record and record[attr] == match:
                    matching_records.append(record)
                    self.wikidata_matches.append(record)
        return matching_records

    def get_pickle_data(self, filename):
        cache_data = {}
        with open(filename, "rb") as file:
            cache_data = pickle.load(file)
            return list(cache_data.values())

    def get_event_series_title(self, filename):
        with open(filename) as file:
            data = json.load(file)

        # Extract all 'title' attributes
        titles = [
            item["title"]["value"]
            for item in data["results"]["bindings"]
            if "title" in item
        ]

        # Print the extracted titles
        return titles

    def get_all_matches(self):
        return self.wikidata_matches
