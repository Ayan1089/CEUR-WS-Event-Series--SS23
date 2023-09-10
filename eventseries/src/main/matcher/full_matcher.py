from typing import List, Optional, Dict

from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventSeries,
    QID,
)


def _is_string_in(string_list: List[str], match_targets: List[str]) -> bool:
    return any(any(string in target for target in match_targets) for string in string_list)


def not_none(list_of_items: List) -> List:
    return [item for item in list_of_items if item is not None]


def _event_matches_series(
    event: WikiDataEvent, event_series: WikiDataEventSeries, ceurws_title: Optional[str] = None
) -> bool:
    string_list = not_none([event.label, event.title, ceurws_title])
    match_targets = not_none([event_series.label, event_series.title])
    return _is_string_in(string_list, match_targets)


def full_matches(
    events: List[WikiDataEvent],
    event_series: List[WikiDataEventSeries],
    ceurws_title: Dict[QID, Optional[str]],
) -> List[FullMatch]:
    # Remove the events that already have a series assigned
    matches = []
    for event in events:
        for series in event_series:
            if _event_matches_series(event, series, ceurws_title[event.qid]):
                matches.append(FullMatch(event=event, series=series, found_by="FullMatch"))

    return matches
