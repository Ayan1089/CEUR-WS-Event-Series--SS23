import json
import os
from pathlib import Path

from plp.ordinal import Ordinal

from eventseries.src.main.query.query_events import WikidataEvents
from eventseries.src.main.util.utility import Utility


class OrdinalExtractor:
    def query_extract_ordinal_from_events(self) -> dict:
        events = WikidataEvents()
        events.query()
        records = events.read_as_dict()
        for record in records:
            Ordinal.addParsedOrdinal(record)
        return records

    def dump_events_with_ordinality(self, records):
        resources_path = Path(__file__).resolve().parent / ".." / "resources"
        with open(
            os.path.join(resources_path, "events_with_ordinal.json"),
            "w",
            encoding="utf-8",
        ) as final:
            json.dump(records, final, default=Utility.serialize_datetime)
