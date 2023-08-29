from pathlib import Path
from typing import List

import pandas as pd

from eventseries.src.main.dblp.event_classes import DblpEventSeries
from eventseries.src.main.dblp.parsing import load_event_series


def is_conference_description(wikidata_event):
    if "description" in wikidata_event and not pd.isna(wikidata_event["description"]):
        return wikidata_event["description"].lower() == "academic conference"
    return False


def match_wikidata_conference_to_series_dblp_id(
        wikidata_df: pd.DataFrame, event_series: List[DblpEventSeries]
):
    def get_series_for_conference(conf_id: str):
        return next(
            (series for series in event_series if conf_id.startswith(series.dblp_id)),
            None,
        )

    wikidata_df = wikidata_df[wikidata_df.dblpEventId.notna()]
    conferences = wikidata_df[
        wikidata_df.apply(is_conference_description, axis=1)
    ].copy()
    conferences["series"] = conferences.dblpEventId.map(get_series_for_conference)
    print(f"Found {len(conferences)} dblp-conference-series for wikidata-conferences")
    return conferences


if __name__ == "__main__":
    path_to_wikidata_events = Path("") / ".." / "resources" / "EventsWithoutSeries.json"
    match_wikidata_conference_to_series_dblp_id(
        pd.read_json(path_to_wikidata_events), load_event_series()
    )
