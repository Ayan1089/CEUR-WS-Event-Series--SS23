import json
import os
import pickle
from typing import List

from eventseries.src.main.dblp.DblpParsing import load_event_series
from eventseries.src.main.dblp.EventClasses import EventSeries
from eventseries.src.main.matcher.nlp_matcher import NlpMatcher


class DblpMatcher:
    def __init__(self):
        self.data = None
        self.nlp_matcher = None

    def remove_existing_matches(self, filename):
        with open(filename) as file:
            self.data = json.load(file)

    def retrieve_dblp_matches(self, wikidata_events_with_dblp_event_id):
        # Retrieve the dblp events
        resources_path = os.path.abspath("resources")
        path = os.path.join(resources_path, "dblp_event_series.pickle")
        dblp_matches = load_event_series()
        dblp_matches = self.load_event_series(path)
        wikidata_events_from_dblp = self.extract_ceur_ws_events_from_dblp(
            dblp_matches, wikidata_events_with_dblp_event_id
        )
        self.nlp_matcher = NlpMatcher(wikidata_events_from_dblp)
        self.nlp_matcher.match()

    def extract_ceur_ws_events_from_dblp(
        self, dblp_matches: List[EventSeries], wikidata_events_with_dblp_event_id
    ):
        """
        Extract all the dblp events that have the same dblpEventId as that of the events in wikidata. Roughly around 2000 records.
        Take these records and pass them to the nlp_matcher to verify the algorithms and then apply them to the unmatched wikidata events.
        """
        dblp_matching_events = []
        for match in dblp_matches:
            for event in wikidata_events_with_dblp_event_id:
                if event == match.dblp_id:
                    dblp_matching_events.append(match)
        # dblp_matching_events = []
        # ctr = 0
        # for match in dblp_matches:
        #     for record in wikidata_events_with_dblp_event_id:
        #         event_titles = []
        #         for event in match.mentioned_events:
        #             event_titles.append(event.title)
        #         if record in event_titles:
        #             dblp_matching_events.append(match)
        #             if ctr < 5:
        #                 print(f"EVENT: {record} and DBLP_MATCH: {match}")
        #                 ctr += 1
        # print("Wikidata events matching with DBLP events: ", len(dblp_matching_events))
        return dblp_matching_events

    # TODO: Use the method from dblp package
    def load_event_series(self, path) -> List[EventSeries]:
        with open(path, "rb") as file:
            event_series: List[EventSeries] = pickle.load(file)
            return event_series
