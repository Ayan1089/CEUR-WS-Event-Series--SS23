"""
Created on 2023-05-03

@author: Ayan1089
"""
import json
import os

from plp.ordinal import Ordinal

from eventseries.src.main.matcher.nlp_matcher import NlpMatcher
from eventseries.src.main.matcher.wikidata_matcher import Matcher
from eventseries.src.main.parsers.event_extractor import EventExtractor
from eventseries.src.main.util.record_attributes import TITLE, LABEL, CEUR_WS_TITLE
from eventseries.src.main.util.utility import Utility
from import2Neo4j import ImportData
from query.queried_events import Events

if __name__ == "__main__":
    importDataTemp = ImportData.ImportData()
    # importDataTemp.fetch_data_and_import()
    # importDataTemp.import_data_to_neo4j(importDataTemp.new_func())
    # importDataTemp.import_event_series_to_neo4j(importDataTemp.new_func_series())
    # importDataTemp.counter_func()
    # importDataTemp.check_event_with_series(importDataTemp.new_func(), importDataTemp.new_func_series())

    """Stored the queried events in resources/events.json """
    events = Events()
    events.query()
    # Query Event series
    events.event_series_query()
    # # todo: Read the json directly using pyLODStorage
    records = events.read_as_dict()
    for record in records:
        Ordinal.addParsedOrdinal(record)
    resources_path = os.path.abspath("resources")
    utility = Utility()
    with open(
        os.path.join(resources_path, "events_with_ordinal.json"), "w", encoding="utf-8"
    ) as final:
        json.dump(records, final, default=Utility.serialize_datetime)

    event_extractor = EventExtractor()
    matcher = Matcher()
    """Remove the events that already have a series assigned"""
    records_without_series = event_extractor.check_events_with_series(records)
    print(
        "Length of the records that do not have series assigned: ",
        len(records_without_series),
    )
    records_with_titles = event_extractor.extract_ceurws_title(records_without_series)
    utility.check_title_label(records_with_titles)
    matches_with_ceurws_titles = matcher.match(records_with_titles, CEUR_WS_TITLE)
    print("Full matches from title of CEUR-WS url: ", len(matches_with_ceurws_titles))

    records_remaining = [
        record
        for record in records_without_series
        if record not in matches_with_ceurws_titles
    ]
    print("RECORDS REMAINING: ", len(records_remaining))

    events_with_wikidata_titles = event_extractor.extract_wikidata_title(
        records_remaining
    )

    matches_with_wikidata_titles = matcher.match(events_with_wikidata_titles, TITLE)
    print(
        "Matches from title of event in wikidata: ", len(matches_with_wikidata_titles)
    )

    records_remaining = [
        record
        for record in records_remaining
        if record not in matches_with_wikidata_titles
    ]
    print("RECORDS REMAINING: ", len(records_remaining))

    """Events having same title and label in wikidata are not required to be matched again"""
    records_with_diff_labels = utility.check_unmatched_titles_labels(records_remaining)
    matches_with_wikidata_labels = matcher.match(
        event_extractor.extract_wikidata_label(records_with_diff_labels), LABEL
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
            records_remaining_with_no_matches, final, default=Utility.serialize_datetime
        )

    events_with_dblp_event_id = [
        event for event in records_remaining_with_no_matches if "dblpEventId" in event
    ]
    print("Records without matches: ", len(records_remaining_with_no_matches))
    print("Records with dblpEventId: ", len(events_with_dblp_event_id))

    print(
        "Matches from label of event in wikidata: ", len(matches_with_wikidata_labels)
    )

    print(
        "Total matches = ",
        len(matches_with_ceurws_titles)
        + len(matches_with_wikidata_titles)
        + len(matches_with_wikidata_labels),
    )

    print(
        len(
            matches_with_ceurws_titles
            and matches_with_wikidata_titles
            and matches_with_wikidata_labels
        )
    )

    ## Import data to neo4j
    # TODO: Enable when local neo4j instance is running
    # import_data = ImportData()
    # import_data.fetch_data_and_import()

    # nlp matches
    nlp_matcher = NlpMatcher(event_extractor, matcher)
    nlp_matcher.match()

    # nlp_matcher.
