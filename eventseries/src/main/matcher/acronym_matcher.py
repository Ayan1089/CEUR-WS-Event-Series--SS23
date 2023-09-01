import json
import os
import re
from typing import List

import pandas as pd

from eventseries.src.main.matcher.phrase_matcher import PhraseMatch


class AcronymMatch:
    def __init__(self, matches_df: pd.DataFrame) -> None:
        self.df = self.create_acronym_df(matches_df)
        self.phrase_matcher = PhraseMatch(self.df)
        self.event_acronyms = []
        self.series_acronyms = []
        self.event_acronyms_titles = {}

    def matcher(self):
        print(f"\nAcronym matcher stats from existing matches:")
        self.phrase_matcher.matcher()

    def wikidata_match(
            self,
            event_titles: List[str],
            series_titles: List[str],
    ) -> List[str]:
        event_acronyms = self.extract_event_acronyms(event_titles)
        series_acronyms = self.extract_series_acronyms()
        # for title in event_titles:
        #     acronym = self.extract_acronym(title)
        #     if acronym is not None:
        #         event_acronyms.append(acronym)
        # for title in series_titles:
        #     acronym = self.extract_acronym(title)
        #     if acronym is not None:
        #         series_acronyms.append(acronym)

        print(f"\nAcronym matcher stats:")
        acronym_matches = self.phrase_matcher.wikidata_match(event_acronyms, series_acronyms)
        matches = []
        for match in acronym_matches:
            if match in self.event_acronyms_titles:
                matches.append(self.event_acronyms_titles[match])
        return matches

    def create_acronym_df(self, matches_df: pd.DataFrame) -> pd.DataFrame:
        acronyms_dict = {}
        for column in ["event", "event_series"]:
            acronyms_dict[column] = matches_df[column].apply(
                lambda x: self.extract_acronym(str(x))
            )

        return pd.DataFrame(acronyms_dict)

    def extract_acronym(self, input_string: str):
        pattern = r"\((.*?)\)"
        matches = re.search(pattern, input_string)

        if matches:
            return matches.group(1)
        else:
            return None

    def extract_event_acronyms(self, event_titles) -> List:
        event_acronyms = []
        resources_path = os.path.abspath("resources")
        events_file = os.path.join(resources_path, "events_without_matches.json")
        with open(events_file) as file:
            events = json.load(file)
        for title in event_titles:
            for event in events:
                if 'title' in event and title in event['title']:
                    if 'acronym' in event and event['acronym'] is not None:
                        event_acronyms.append(event['acronym'])
                        self.event_acronyms_titles[event['acronym']] = title
            event_acronyms.append(self.extract_acronym(title))
        event_acronyms = list(filter(lambda item: item is not None, event_acronyms))
        return event_acronyms

    def extract_series_acronyms(self) -> List:
        resources_path = os.path.abspath("resources")
        series_file = os.path.join(resources_path, "event_series.json")
        with open(series_file) as file:
            series_list = json.load(file)
        series_acronyms = [item["acronym"]["value"] for item in series_list["results"]["bindings"] if "acronym" in item]
        series_acronyms = list(filter(lambda item: item is not None, series_acronyms))
        return series_acronyms
