from typing import Dict

from eventseries.src.main.dblp.event_classes import DblpEvent, DblpEventSeries
from eventseries.src.main.repository.completion_cache import CompletionCache
from eventseries.src.main.repository.completions import Match
from eventseries.src.main.repository.dblp_respository import DblpRepository
from eventseries.src.main.repository.wikidata_dataclasses import (
    QID,
    WikiDataEvent,
    WikiDataEventSeries,
    WikiDataProceeding,
    WikiDataEventType,
)
from eventseries.src.main.repository.wikidata_query_manager import WikiDataQueryManager


class Repository:
    def __init__(
        self,
        query_manager: WikiDataQueryManager,
        dblp_repo: DblpRepository,
        completion_cache: CompletionCache,
    ):
        self.query_manager: WikiDataQueryManager = query_manager
        self.dblp_repo: DblpRepository = dblp_repo
        self.events_by_qid: Dict[QID, WikiDataEvent] = {
            item.qid: item for item in self.query_manager.wikidata_all_ceurws_events()
        }
        self.event_series_by_qid: Dict[QID, WikiDataEventSeries] = {
            item.qid: item for item in self.query_manager.wikidata_all_ceurws_event_series()
        }
        self.proceeding_by_qid: Dict[QID, WikiDataProceeding] = {
            item.qid: item for item in self.query_manager.wikidata_all_proceedings()
        }
        self.proceeding_by_event_qid: Dict[QID, WikiDataProceeding] = {
            item.event: item for item in self.proceeding_by_qid.values()
        }
        self.completion_cache = completion_cache

        self._add_type_to_events_and_series()

    def matches_by_event_qid(self):
        return {match.event.qid: match for match in self.completion_cache.get_all_matches()}

    def get_event_by_qid(self, qid: QID, patched: bool = True) -> WikiDataEvent:
        raw_event = self.events_by_qid[qid]
        if not patched:
            return raw_event
        for completion in self.completion_cache.get_completions_for_qid(qid):
            completion.patch_item(raw_event)
        return raw_event

    def get_event_series_by_qid(self, qid: QID, patched: bool = True) -> WikiDataEventSeries:
        raw_series = self.event_series_by_qid[qid]
        if not patched:
            return raw_series
        for completion in self.completion_cache.get_completions_for_qid(qid):
            completion.patch_item(raw_series)
        return raw_series

    def get_proceeding_by_qid(self, qid: QID, patched: bool = True) -> WikiDataProceeding:
        raw_series = self.proceeding_by_qid[qid]
        if not patched:
            return raw_series
        for completion in self.completion_cache.get_completions_for_qid(qid):
            completion.patch_item(raw_series)
        return raw_series

    def get_dblp_event_by_id(self, dblp_id: str) -> DblpEvent:
        return self.dblp_repo.get_or_load_event(dblp_id)


    def get_dblp_event_series_by_id(self, dblp_id: str) -> DblpEventSeries:
        return self.dblp_repo.get_or_load_event_series(dblp_id)


    def get_matches(self) -> list[Match]:
        return self.completion_cache.get_all_matches()


    def events_without_series(self, ignore_match_completions: bool = False):
        events_without_series = [
            event for event in self.events_by_qid.values() if event.part_of_series is None
        ]
        if ignore_match_completions:
            return events_without_series
        matches_dict = self.matches_by_event_qid()
        return [event for event in events_without_series if event.qid not in matches_dict]


    def _add_type_to_events_and_series(self):
        for conf_series in self.query_manager.wikidata_conference_series():
            series = self.event_series_by_qid.get(conf_series.qid)
            series.type = WikiDataEventType.CONFERENCE
        for conf in self.query_manager.wikidata_conferences():
            event = self.events_by_qid.get(conf.qid)
            event.type = WikiDataEventType.CONFERENCE
        for workshop_series in self.query_manager.wikidata_workshop_series():
            series = self.event_series_by_qid.get(workshop_series.qid)
            series.type = WikiDataEventType.WORKSHOP
        for workshop in self.query_manager.wikidata_workshops():
            event = self.events_by_qid.get(workshop.qid)
            event.type = WikiDataEventType.WORKSHOP
