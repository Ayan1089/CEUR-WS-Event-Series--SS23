"""
Created on 2023-05-03

@author: Ayan1089
"""

from eventseries.src.main.completeSeries.series_completion import SeriesCompletion
from eventseries.src.main.matcher.full_matcher import FullMatch
from eventseries.src.main.matcher.nlp_matcher import NlpMatcher

# from eventseries.src.main.matcher.nlp_matcher import NlpMatcher
from eventseries.src.main.matcher.wikidata_matcher import Matcher
from eventseries.src.main.parsers.event_extractor import EventExtractor
from eventseries.src.main.parsers.ordiniality_extractor import OrdinalExtractor
from eventseries.src.main.query.query_series import WikidataEventSeries
from eventseries.src.main.util.utility import Utility

if __name__ == "__main__":
    # importDataTemp = ImportData.ImportData()
    # importDataTemp.fetch_data_and_import()
    # importDataTemp.import_data_to_neo4j(importDataTemp.new_func())
    # importDataTemp.import_event_series_to_neo4j(importDataTemp.new_func_series())
    # importDataTemp.counter_func()
    # importDataTemp.check_event_with_series(importDataTemp.new_func(), importDataTemp.new_func_series())

    # Query event series
    event_series = WikidataEventSeries()
    event_series.event_series_query()

    # Query events and extract ordinality
    extractor = OrdinalExtractor()
    records = extractor.query_extract_ordinal_from_events()
    extractor.dump_events_with_ordinality(records)

    # Extract full matches
    utility = Utility()
    event_extractor = EventExtractor()
    matcher = Matcher()
    full_matcher = FullMatch(utility, event_extractor, matcher)
    full_matcher.match(records)

    # Use case scenario 1
    series_completion = SeriesCompletion()
    event_series = series_completion.get_event_series_from_ceur_ws_proceedings()

    # nlp matches
    nlp_matcher = NlpMatcher(event_extractor, matcher)
    results_df = nlp_matcher.match(utility.read_event_titles(), utility.read_event_series_titles())

    # Dumping results into a json
    results_df.to_json("/Users/ayan/Projects/KGLab/main/CEUR-WS-Event-Series--SS23/eventseries/src/main/resources/results.json")
