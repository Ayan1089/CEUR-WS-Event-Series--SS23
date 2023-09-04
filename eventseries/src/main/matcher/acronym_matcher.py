import re

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
            events_df: pd.DataFrame,
            events_series_df: pd.DataFrame,
    ) -> pd.DataFrame:
        events_df = self.extract_event_acronyms(events_df)
        series_df = self.extract_series_acronyms(events_series_df)
        # for title in event_titles:
        #     acronym = self.extract_acronym(title)
        #     if acronym is not None:
        #         event_acronyms.append(acronym)
        # for title in series_titles:
        #     acronym = self.extract_acronym(title)
        #     if acronym is not None:
        #         series_acronyms.append(acronym)
        event_acronyms_df = events_df.drop(columns="title")
        event_acronyms_df = event_acronyms_df.rename(columns={"event_acronyms": "title"})

        series_acronyms_df = series_df.drop(columns="title")
        series_acronyms_df = series_acronyms_df.rename(columns={"series_acronyms": "title"})

        print(f"\nAcronym matcher stats:")
        acronym_matches_df = self.phrase_matcher.wikidata_match(event_acronyms_df, series_acronyms_df)

        return acronym_matches_df

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

    def extract_event_acronyms(self, events_df: pd.DataFrame) -> pd.DataFrame:
        # event_acronyms = []
        # event_ids = []
        # resources_path = os.path.abspath("resources")
        # events_file = os.path.join(resources_path, "events_without_matches.json")
        # with open(events_file) as file:
        #     events = json.load(file)
        # for i in range(0, len(events_df["title"])):
        #     for event in events:
        #         if 'title' in event and events_df.loc[i, "title"] in event['title']:
        #             if 'acronym' in event and event['acronym'] is not None:
        #                 event_acronyms.append(event['acronym'])
        #                 print("##########")
        #                 print(events_df.loc[i, "title"])
        #                 self.event_acronyms_titles[event['acronym']] = events_df.loc[i, "title"]
        #     event_acronyms.append(self.extract_acronym(events_df.loc[i, "title"]))
        #     event_ids.append(events_df.loc[i, "event_id"])
        # events_acronyms_df = pd.DataFrame({"title": event_acronyms, "event_id": events_df})
        # events_acronyms_df.dropna(inplace=True)

        for i in range(0, len(events_df)):
            if events_df.loc[i, "event_acronyms"] is None:
                events_df.loc[i, "event_acronyms"] = self.extract_acronym(events_df.loc[i, "title"])
        return events_df

    def extract_series_acronyms(self, event_series_df: pd.DataFrame) -> pd.DataFrame:
        # resources_path = os.path.abspath("resources")
        # series_acronyms = []
        # series_id = []
        # series_file = os.path.join(resources_path, "event_series.json")
        # with open(series_file) as file:
        #     series_list = json.load(file)
        # for item in series_list["results"]["bindings"]:
        #     if "acronym" in item:
        #         series_acronyms.append(item["acronym"]["value"])
        #         series_id.append(item["series"]["value"])
        # series_acronyms = list(filter(lambda item: item is not None, series_acronyms))
        # series_acronyms_df = pd.DataFrame({"title": series_acronyms, "series_id": series_id})
        # series_acronyms_df.dropna(inplace=True)
        # return series_acronyms_df
        for i in range(0, len(event_series_df)):
            if event_series_df.loc[i, "series_acronyms"] is None:
                event_series_df.loc[i, "series_acronyms"] = self.extract_acronym(event_series_df.loc[i, "title"])
        return event_series_df
