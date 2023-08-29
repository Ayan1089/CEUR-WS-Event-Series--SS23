from importlib import resources as ires
from pathlib import Path
from typing import List, Dict, Optional

from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.dblp.event_classes import DblpEvent, DblpEventSeries
from eventseries.src.main.dblp.parsing import (
    dblp_event_from_html_content,
    dbpl_event_series_from_html_content,
)
from eventseries.src.main.repository.cached_online_context import CachedContext


class DblpRepository(CachedContext):
    EVENTS = "events"
    EVENT_SERIES = "event_series"

    def __init__(
        self,
        dblp_context: DblpContext,
        resource_dir: Path = ires.files("eventseries.src.main") / "resources" / "dblp" / "parsed",
        load_on_init: bool = True,
        store_on_delete: bool = True,
    ):
        self.ctx = dblp_context
        # Store events and event_series indexed by their dblp_id
        super().__init__(resource_dir, load_on_init, store_on_delete)
        # events and event_series might have been loaded from disk
        self.events: Dict[str, DblpEvent] = self.cache.get(DblpRepository.EVENTS, {})
        self.event_series: Dict[str, DblpEventSeries] = self.cache.get(
            DblpRepository.EVENT_SERIES, {}
        )
        self.matched: Dict[DblpEvent, DblpEventSeries] = {}
        # make sure that the cache tracks the objects and not the other way around
        self.cache[DblpRepository.EVENTS] = self.events
        self.cache[DblpRepository.EVENT_SERIES] = self.event_series

    def load_cached(self):
        super().load_cached()
        self.events = self.cache.get(DblpRepository.EVENTS, {})
        self.event_series = self.cache.get(DblpRepository.EVENT_SERIES, {})

    def load_cached_file(self, build_dict, file_path: Path):
        if file_path.stem in (DblpRepository.EVENTS, DblpRepository.EVENT_SERIES):
            build_dict[file_path.stem] = CachedContext._load_pickle(file_path)
        else:
            build_dict[file_path.stem] = CachedContext._load_json(file_path)

    def store_content_to_file(self, file_path, file_content, overwrite: bool):
        if file_path.stem in (DblpRepository.EVENTS, DblpRepository.EVENT_SERIES):
            super()._store_pickle(file_path, file_content, overwrite)
        else:
            super()._store_json(file_content, file_path, overwrite)

    def update_event_series_from_context(self, dblp_id: Optional[str] = None):
        event_series_ids = (
            self.ctx.get_cached_series_keys() if dblp_id is None else [self.ctx.get_cached(dblp_id)]
        )
        self.remove_irregular_series(event_series_ids)
        series_contents = {series: self.ctx.get_cached(series) for series in event_series_ids}

        updated_series = {
            series: dbpl_event_series_from_html_content(series_contents[series])
            for series in series_contents
        }
        self.event_series.update(updated_series)

    def cache_event_series(self, dblp_id: str, event_series: DblpEventSeries):
        self.event_series[dblp_id] = event_series

    def cache_event(self, event: DblpEvent):
        self.events[event.dblp_id] = event

    def get_or_load_event(self, dblp_id) -> DblpEvent:
        if dblp_id not in self.events:
            html = self.ctx.request_or_load_dblp(dblp_db_entry=dblp_id)
            dblp_event = dblp_event_from_html_content(html, dblp_id)
            self.events[dblp_id] = dblp_event

        return self.events[dblp_id]

    def get_or_load_event_series(self, dblp_id) -> DblpEventSeries:
        if dblp_id not in self.event_series:
            html = self.ctx.request_or_load_dblp(dblp_db_entry=dblp_id)
            dblp_event_series: DblpEventSeries = dbpl_event_series_from_html_content(html, dblp_id)
            self.event_series[dblp_id] = dblp_event_series

        return self.event_series[dblp_id]

    def get_events_for_series(self, series_dblp_id: str) -> List[DblpEvent]:
        if not self.ctx.is_cached(series_dblp_id):
            self.ctx.request_or_load_dblp(series_dblp_id)
        return [
            self.get_or_load_event(dblp_id)
            for dblp_id in self.ctx.get_events_for_series(series_dblp_id)
        ]

    @staticmethod
    def remove_irregular_series(event_series_ids: List[str]):
        """exclude Festschriften: Birthdays, In Memory of ..., In Honor of ..."""
        return [series for series in event_series_ids if series not in ("conf/birthday", "conf/ac")]
