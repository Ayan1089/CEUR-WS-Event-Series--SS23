import logging
from typing import List

import pandas as pd

from eventseries.src.main.completion.attribute_completion import extract_acronym
from eventseries.src.main.matcher.phrase_matcher import PhraseMatch
from eventseries.src.main.repository.completions import FullMatch
from eventseries.src.main.repository.wikidata_dataclasses import WikiDataEvent, WikiDataEventSeries


def _find_acronyms(matches_df: pd.DataFrame) -> pd.DataFrame:
    acronym_df = pd.DataFrame()
    acronym_df["event"] = matches_df["event"].map(extract_acronym)
    acronym_df["series"] = matches_df["series"].map(extract_acronym)

    # Filter out rows where at least one value is None
    return acronym_df.dropna()


class AcronymMatch:
    def __init__(self, matches_df: pd.DataFrame) -> None:
        self.acronym_df = _find_acronyms(matches_df)
        self.phrase_matcher = PhraseMatch(self.acronym_df)

    def test_accuracy(self):
        logging.debug("Acronym matcher only trains phrase matcher.")
        self.phrase_matcher.test_accuracy()

    def wikidata_match(
        self, events: List[WikiDataEvent], event_series: List[WikiDataEventSeries]
    ) -> List[FullMatch]:
        events_with_acronym = [event for event in events if event.acronym is not None]
        event_series = [series for series in event_series if series.acronym is not None]

        acronym_matches_df = self.phrase_matcher.wikidata_match(events_with_acronym, event_series)

        return acronym_matches_df
