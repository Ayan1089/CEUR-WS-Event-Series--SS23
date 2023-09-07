import json
import os
from pathlib import Path

from eventseries.src.main.matcher.wikidata_matcher import Matcher
from eventseries.src.main.parsers.event_extractor import EventExtractor
from eventseries.src.main.util.record_attributes import CEUR_WS_TITLE, TITLE, LABEL
from eventseries.src.main.util.utility import Utility


class FullMatch:
    def __init__(
        self, utility: Utility, event_extractor: EventExtractor, matcher: Matcher
    ) -> None:
        self.utility = utility
        self.event_extractor = event_extractor
        self.matcher = matcher

    def match(self, records):
        resources_path = Path(__file__).resolve().parent / ".." / "resources"

        # Remove the events that already have a series assigned
        records_without_series = self.event_extractor.get_events_without_series(records)
        print(
            "Length of the records that do not have series assigned: ",
            len(records_without_series),
        )
        records_with_titles = self.event_extractor.extract_ceurws_title(
            records_without_series
        )
        self.utility.check_title_label(records_with_titles)
        matches_with_ceurws_titles = self.matcher.match(
            records_with_titles, CEUR_WS_TITLE
        )
        print(
            "Full matches from title of CEUR-WS url: ", len(matches_with_ceurws_titles)
        )

        records_remaining = [
            record
            for record in records_without_series
            if record not in matches_with_ceurws_titles
        ]
        print("RECORDS REMAINING: ", len(records_remaining))

        events_with_wikidata_titles = self.event_extractor.extract_wikidata_title(
            records_remaining
        )

        matches_with_wikidata_titles = self.matcher.match(
            events_with_wikidata_titles, TITLE
        )
        print(
            "Matches from title of event in wikidata: ",
            len(matches_with_wikidata_titles),
        )

        records_remaining = [
            record
            for record in records_remaining
            if record not in matches_with_wikidata_titles
        ]
        print("RECORDS REMAINING: ", len(records_remaining))

        """Events having same title and label in wikidata are not required to be matched again"""
        records_with_diff_labels = self.utility.check_unmatched_titles_labels(
            records_remaining
        )
        matches_with_wikidata_labels = self.matcher.match(
            self.event_extractor.extract_wikidata_label(records_with_diff_labels), LABEL
        )
        print(
            "Matches from label of event in wikidata: ",
            len(matches_with_wikidata_labels),
        )
        records_remaining_with_no_matches = [
            record
            for record in records_remaining
            if record not in matches_with_wikidata_labels
        ]
        """Dump events where no matches are found"""
        with open(
            os.path.join(resources_path, "events_without_matches.json"),
            "w",
            encoding="utf-8",
        ) as final:
            json.dump(
                records_remaining_with_no_matches,
                final,
                default=Utility.serialize_datetime,
            )

        events_with_dblp_event_id = [
            event
            for event in records_remaining_with_no_matches
            if "dblpEventId" in event
        ]
        print()
        print("Records without matches: ", len(records_remaining_with_no_matches))
        print("Records with dblpEventId: ", len(events_with_dblp_event_id))

        print(
            "Total full matches: ",
            len(matches_with_ceurws_titles)
            + len(matches_with_wikidata_titles)
            + len(matches_with_wikidata_labels),
        )

        # print(
        #    len(
        #        matches_with_ceurws_titles
        #        and matches_with_wikidata_titles
        #        and matches_with_wikidata_labels
        #   )
        # )
