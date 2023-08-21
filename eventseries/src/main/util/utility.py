import datetime
import json
import os
import re

# from eventseries.src.main.dblp.EventClasses import EventSeries
from eventseries.src.main.util.record_attributes import CEUR_WS_TITLE, LABEL, TITLE


class Utility(object):
    @staticmethod
    def serialize_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    def generate_ceur_spt_url(self, url):
        prefix = "http://ceur-ws.org/Vol-"
        volume_number = self.extract_vol_number(prefix, url)
        if volume_number is None:
            prefix = "https://ceur-ws.org/Vol-"
            volume_number = self.extract_vol_number(prefix, url)
        return (
            "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-" + volume_number + ".json"
        )

    def generate_ceurws_url(self, url):
        prefix = "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-"
        volume_number = self.extract_vol_number(prefix, url)
        return "https://ceur-ws.org/Vol-" + volume_number

    def extract_vol_number(self, prefix, url):
        pattern = re.escape(prefix) + r"(\d+)"
        match = re.search(pattern, url)
        if match:
            return match.group(1)
        else:
            return None

    def check_unmatched_titles_labels(self, records):
        records_with_diff_labels = []
        for record in records:
            if TITLE in record and LABEL in record:
                if record[TITLE] != record[LABEL]:
                    records_with_diff_labels.append(record)
        return records_with_diff_labels

    def check_title_label(self, records):
        """Assuming the ground truth is the title in CEUR-WS URL"""
        count_diff_titles = 0
        count_diff_labels = 0
        for record in records:
            if (CEUR_WS_TITLE and TITLE in record) and (
                record[CEUR_WS_TITLE] != record[TITLE]
            ):
                count_diff_titles += 1
            if (CEUR_WS_TITLE and LABEL in record) and (
                record[CEUR_WS_TITLE] != record[LABEL]
            ):
                count_diff_labels += 1
        print(
            "Number of records with different CEUR-WS titles and wikidata titles: ",
            count_diff_titles,
        )
        print(
            "Number of records with different CEUR-WS titles and wikidata labels: ",
            count_diff_labels,
        )
        print(
            "Number of records with different wikidata titles and wikidata labels: ",
            len(self.check_unmatched_titles_labels(records)),
        )

    """One-to-one mapping dblp events to events series """

    # def event_titles_to_event_series(self, events_dataset: List[EventSeries]):
    #     matches = {}
    #     for event_series in events_dataset:
    #         for event in event_series.mentioned_events:
    #             matches[event] = event_series.name
    #     return matches

    def read_event_titles(self):
        """Json reader for wikidata event titles"""
        events_file = os.path.join(
            os.path.abspath("resources"), "events_without_matches.json"
        )
        with open(events_file) as file:
            events = json.load(file)
            event_titles = [item["title"] for item in events if "title" in item]
        return event_titles

    def read_event_acronyms(self):
        """Json reader for wikidata event titles"""
        events_file = os.path.join(
            os.path.abspath("resources"), "events_without_matches.json"
        )
        with open(events_file) as file:
            events = json.load(file)
            event_titles = [item["acronym"] for item in events if "acronym" in item]
        return event_titles


    def read_event_series_titles(self):
        """Json reader for wikidata event series titles"""
        series_file = os.path.join(os.path.abspath("resources"), "event_series.json")
        with open(series_file) as file:
            series = json.load(file)
            series_titles = [
                item["title"]["value"]
                for item in series["results"]["bindings"]
                if "title" in item
            ]
        return series_titles

    def read_event_series_acronyms(self):
        """Json reader for wikidata event series titles"""
        series_file = os.path.join(os.path.abspath("resources"), "event_series.json")
        with open(series_file) as file:
            series = json.load(file)
            series_titles = [
                item["title"]["value"]
                for item in series["results"]["bindings"]
                if "title" in item
            ]
        return series_titles
