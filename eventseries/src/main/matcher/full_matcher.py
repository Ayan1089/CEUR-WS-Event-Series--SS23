from typing import List

from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import WikiDataEvent, WikiDataEventSeries


def _is_string_in(string_list: List[str], match_targets: List[str]) -> bool:
    return any(any(string in target for target in match_targets) for string in string_list)


def _event_matches_series(event: WikiDataEvent, event_series: WikiDataEventSeries) -> bool:
    string_list = [event.label, event.title] if event.title is not None else [event.label]
    match_targets = (
        [event_series.label, event_series.title]
        if event_series.title is not None
        else [event_series.label]
    )
    return _is_string_in(string_list, match_targets)


def full_matches(
    events: List[WikiDataEvent], event_series: List[WikiDataEventSeries]
) -> List[FullMatch]:
    # Remove the events that already have a series assigned
    matches = []
    for event in events:
        for series in event_series:
            if _event_matches_series(event, series):
                matches.append(FullMatch(event=event, series=series, found_by="FullMatch"))

    return matches
