from typing import Dict

from easydict import EasyDict as edict

from eventseries.src.main.dblp.event_classes import DblpEvent
from eventseries.src.main.repository.completion_cache import CompletionCache
from eventseries.src.main.repository.completions import Match
from eventseries.src.main.repository.dblp_respository import DblpRepository
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
        self.events_by_qid: Dict[str, edict] = {
            item["event"]: item for item in self.query_manager.wikidata_all_ceurws_events()
        }
        self.event_series_by_qid: Dict[str, edict] = {
            item["series"]: item for item in self.query_manager.wikidata_all_ceurws_event_series()
        }
        self.completion_cache = completion_cache

    def get_event_by_qid(self, qid: str, patched: True) -> edict:
        raw_event = self.events_by_qid.get(qid)
        if not patched:
            return raw_event
        for completion in self.completion_cache.get_completions_for_qid(qid):
            completion.patch_event(raw_event)
        return raw_event

    def get_event_series_by_qid(self, qid: str) -> edict:
        return self.event_series_by_qid.get(qid)

    def get_dblp_event_by_id(self, dblp_id: str) -> DblpEvent:
        return self.dblp_repo.get_or_load_event(dblp_id)

    def get_matches(self) -> list[Match]:
        return self.completion_cache.get_all_matches()
