import logging
from typing import Set, List

from eventseries.src.main.completion.check_annual_proceeding import (
    CheckAnnualProceeding,
)
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import get_title_else_label


class SeriesCompletion:

    def __init__(self, repository: Repository) -> None:
        self.annual_proceedings = []
        self.repo = repository

    def get_event_series_from_ceur_ws_proceedings(self) -> Set:
        proceedings = self.repo.proceeding_by_qid.values()
        # Check annual proceeding
        event_series = []
        annual_proceeding = CheckAnnualProceeding()
        for proceeding in proceedings:
            title = get_title_else_label(proceeding)
            if annual_proceeding.is_proceeding_annual(title):
                self.annual_proceedings.append(proceeding)
        logging.info("Found proceedings with `annual` synonyms : %s", len(self.annual_proceedings))
        for proceeding in self.annual_proceedings:
            event  =self.repo.get_event_by_qid(proceeding.event)
            if event.part_of_series is not None:
                event_series.append(event.part_of_series)
        set_event_series = set(event_series)
        logging.info("Unique series found in wikidata: %s", len(set_event_series))
        return set_event_series

    def get_annual_proceedings(self) -> List[str]:
        return self.annual_proceedings
