from typing import Dict, Set

from eventseries.src.main.completeSeries.check_annual_proceeding import (
    CheckAnnualProceeding,
)
from eventseries.src.main.query.query_proceedings import WikidataEventsProceedings


class SeriesCompletion:
    def get_event_series_from_ceur_ws_proceedings(self) -> Set:
        # TODO: Check the fastest way to retrieve the CEUR-WS proccedings
        events_dict = self.extract_proceedings_titles()
        # Check annual proceeding
        """ Both JsonCacheManager(CEUR-WS proceedings) and CEUR-WS proceedings from wikidata give
        the same number of results having annual keyword.(Seperatly matcher is not required to be called for the
        CEUR-WS proceedings)
        """
        event_series = list()
        annual_proceeding = CheckAnnualProceeding()
        annual_proceedings = list()
        for event in events_dict:
            if "proceedingTitle" in event and annual_proceeding.is_proceeding_annual(
                event["proceedingTitle"]
            ):
                annual_proceedings.append(event)
        print(f"\nFound proceedings with `annual` synonyms : ", len(annual_proceedings))
        for event in annual_proceedings:
            if "series" in event:
                event_series.append(event["series"])
        set_event_series = set(event_series)
        print(f"Found series in wikidata: ", len(set_event_series))
        print()
        return set_event_series

    def extract_proceedings_titles(self) -> Dict:
        events = WikidataEventsProceedings()
        events_dict = events.read_as_dict()
        # cache_manager = JsonCacheManager()
        # list_events = cache_manager.load_lod("volumes")
        return events_dict