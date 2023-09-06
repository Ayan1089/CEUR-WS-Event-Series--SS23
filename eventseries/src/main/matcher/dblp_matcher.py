import logging
import multiprocessing
import re
from typing import List, Dict, Optional, Sequence, Callable, Union

from math import ceil

from eventseries.src.main.dblp.dblp_context import get_dblp_id_from_url
from eventseries.src.main.dblp.event_classes import DblpEvent, DblpEventSeries
from eventseries.src.main.dblp.scraper import DblpScraper
from eventseries.src.main.dblp.venue_information import HasPart
from eventseries.src.main.repository.completions import FullMatch, NameMatch, DblpMatch, Match
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventType,
    WikiDataEventSeries,
)


def _extract_possible_series_abbreviation(event_title: str) -> Optional[str]:
    # Often the title says that the workshop is part of a conference like workshop@conference.
    at_pattern = r"(\w+)@\w+"
    match = re.search(at_pattern, event_title)
    if not match:
        return None
    return match.group(1)


def get_next_or_none(sequence: Sequence, predicate: Callable):
    return next((item for item in sequence if predicate(item)), None)


class DblpMatcher:
    def __init__(self, repository: Repository, to_be_matched: Optional[List[WikiDataEvent]] = None):
        self.repo = repository
        self.to_be_matched = (
            to_be_matched
            if to_be_matched is not None
            else self.repo.events_without_series(ignore_match_completions=True)
        )
        self.with_dblp_id = [event for event in self.to_be_matched if event.dblp_id is not None]
        self.dbpl_to_wikidata: Dict[DblpEvent, WikiDataEvent] = {
            self.repo.get_dblp_event_by_id(event.dblp_id): event for event in self.with_dblp_id
        }

    def match_through_dblp(self) -> List[Match]:
        """Match dblp events (extracted in init) to dblp series.
        Differentiate between conferences and workshops.
        Use multiprocessing to handle high number of events.
        :returns all found matches, possibly FullMatches, DblpMatches and NameMatches."""
        nbr_of_dblp_events = len(self.dbpl_to_wikidata.keys())
        if nbr_of_dblp_events > 500:
            cpu_count = multiprocessing.cpu_count()
            chunk_size = ceil(nbr_of_dblp_events / cpu_count)
            final_list: List[Match] = []
            with multiprocessing.Pool(cpu_count) as pool:
                for result in pool.imap_unordered(
                    func=self._match_through_dblp,
                    iterable=self.dbpl_to_wikidata.keys(),
                    chunksize=chunk_size,
                ):
                    if result is not None:
                        final_list.append(result)
            return final_list
        # Don't use multiprocessing.
        return list(
            filter(
                lambda x: x is not None, map(self._match_through_dblp, self.dbpl_to_wikidata.keys())
            )
        )

    def _match_through_dblp(self, event: DblpEvent) -> Optional[Match]:
        """
        Try to find a match for this event. Depending on whether it is a conference or workshop
         different strategies will be applied.
        :param event: the DblpEvent for which a match should be found.
        It should be a key in self.dbpl_to_wikidata.
        :return: A match (FullMatch, DblpMatch, NameMatch) or None if nothing could be found.
        """
        scraper = DblpScraper(ctx=self.repo.dblp_repo.ctx)
        match: Optional[Match] = None
        if self.dbpl_to_wikidata[event].type == WikiDataEventType.CONFERENCE:
            match = self._find_series_for_conference(event, scraper)
        elif self.dbpl_to_wikidata[event].type == WikiDataEventType.WORKSHOP:
            match = self._find_series_for_workshop(event, scraper)
        else:
            logging.info("Skipping event with unknown type: %s", event)
        if match is not None:
            if isinstance(match, DblpMatch):
                return self.find_full_match_from_dblp_match(match)
            return match
        return None

    def match_dblp_series_id_to_wikidata(
        self, dblp_series_id: str
    ) -> Optional[WikiDataEventSeries]:
        """Try to match a dblp series to a wikidata series.
        1. Try to find a wikidata series that has the same dblp id.
        2. Try to find a wikidata series that has similar title, abbreviation.
        TODO possibly use all series from wikidata (not only ceurws) and use better matching.
        :returns a WikiDataEventSeries if one could be found else None
        """
        wikidata_series: List[WikiDataEventSeries] = list(self.repo.event_series_by_qid.values())

        # Try to match it through wikidata series that have a dblp-id.
        with_dblp_id = [series for series in wikidata_series if series.dblp_id is not None]
        opt_match: Optional[WikiDataEventSeries] = get_next_or_none(
            with_dblp_id, predicate=lambda series: series.dblp_id == dblp_series_id
        )
        if opt_match is not None:
            return opt_match

        # Try to find a wikidata series that has the same acronym or title
        dblp_series = self.repo.get_dblp_event_series_by_id(dblp_series_id)
        for series in wikidata_series:
            if series.title is not None and dblp_series.name in series.title:
                return series
            if series.acronym is not None and dblp_series.abbreviation is not None:
                if dblp_series.abbreviation == series.acronym:
                    return series
            if dblp_series.name in series.label:
                return series
        return None

    def find_full_match_from_dblp_match(self, dblp_match: DblpMatch) -> Union[DblpMatch, FullMatch]:
        opt_series = self.match_dblp_series_id_to_wikidata(dblp_match.series.dblp_id)
        if opt_series:
            return FullMatch(
                event=dblp_match.event, series=opt_series, found_by=dblp_match.found_by
            )
        return dblp_match

    def _find_series_for_workshop(self, event: DblpEvent, scraper: DblpScraper) -> Optional[Match]:
        """Try to identify the series which the workshop is part of.
        1. Filter possible parents that are likely workshops
        2. If only one parent remains return it.
        3. Extract a possible abbreviation of the parent series from the title.
        4. Try to find series in VenueInformation
        5. Try to directly find the series from the abbreviation
        :returns The dblp_id of the parent or None if no parent could be identified.
        """
        possible_parents: List[Dict] = scraper.extract_parents_from_dblp_event_id(event.dblp_id)
        if len(possible_parents) == 0:
            logging.warning("Could not find any parents in dblp for: %s", event)
        parents_with_workshop_in_title: List[Dict] = [
            parent
            for parent in possible_parents
            if self._series_contains_workshop_in_title(parent["dblp_id"])
        ]
        if len(parents_with_workshop_in_title) == 1:
            dblp_id_of_series = parents_with_workshop_in_title[0]["dblp_id"]
            return DblpMatch(
                event=self.dbpl_to_wikidata[event],
                series=self.repo.dblp_repo.get_or_load_event_series(dblp_id_of_series),
                found_by="DblpMatcher::_find_parent_for_workshop",
            )
        # Try to find it though the abbreviation of the
        possible_parent_abbreviation = _extract_possible_series_abbreviation(event.title)
        # Test whether the abbreviation can be found in the has_part venue info of a parent.
        if possible_parent_abbreviation is None:
            return None
        # Convert to lower case for safer string comparisons
        possible_parent_abbreviation = possible_parent_abbreviation.lower()

        opt_match: Union[DblpMatch, NameMatch, None] = (
            self._find_series_of_workshop_through_venue_info(
                event, possible_parent_abbreviation, possible_parents
            )
        )
        if opt_match is not None:
            return opt_match
        # Try to find a parent with matching abbreviation
        parent_with_abbreviation = [
            parent
            for parent in parents_with_workshop_in_title
            if possible_parent_abbreviation in parent["name"]
            or possible_parent_abbreviation in parent["dblp_id"]
        ]
        if len(parent_with_abbreviation) == 1:
            series_id = parent_with_abbreviation[0]["dblp_id"]
            return DblpMatch(
                event=self.dbpl_to_wikidata[event],
                series=self.repo.get_dblp_event_series_by_id(series_id),
                found_by="DblpMatcher::abbreviation",
            )
        return None
        #return self._find_series_for_workshop_from_constructed_url(
        #    possible_parent_abbreviation, event
        #)

    def _find_series_for_workshop_from_constructed_url(
        self, possible_abbreviation: str, event: DblpEvent
    ) -> Optional[DblpMatch]:
        # Try to build a dblp-id from the abbreviation
        constructed_id = "conf/" + possible_abbreviation
        try:
            series = self.repo.get_dblp_event_series_by_id(dblp_id=constructed_id)
            return DblpMatch(
                event=self.dbpl_to_wikidata[event],
                series=series,
                found_by="DblpMatcher::abbreviation::constructed_id",
            )
        except ValueError:
            logging.info("Failed to resolve dblp_id from abbreviation %s", possible_abbreviation)
        return None

    def _find_series_of_workshop_through_venue_info(
        self, event, parent_abbreviation: str, all_parents: List[Dict]
    ) -> Optional[Union[DblpMatch, NameMatch]]:
        series_list = [
            self.repo.dblp_repo.get_or_load_event_series(parent["dblp_id"])
            for parent in all_parents
        ]
        has_parts: List[HasPart] = []
        for series in series_list:
            if series.venue_information:
                has_parts.extend(series.venue_information.has_part)

        matching_candidates = [
            has_part for has_part in has_parts if parent_abbreviation in has_part.part.name.lower()
        ]
        for has_part in matching_candidates:
            # If both event and has_part define a year it has to match.
            if has_part.years is not None and event.year is not None:
                if event.year not in has_part.years:
                    logging.info(
                        "Dismissing match based on wrong year."
                        "Event (%s) had year %s but has_part was only defined for %s.",
                        event,
                        event.year,
                        has_part.years,
                    )
                    continue
            # If has part defines a reference the DblpMatch can be returned
            wiki_event: WikiDataEvent = self.dbpl_to_wikidata[event]
            if has_part.part.reference is not None:
                dblp_id = get_dblp_id_from_url(has_part.part.reference)
                return DblpMatch(
                    event=wiki_event,
                    series=self.repo.dblp_repo.get_or_load_event_series(dblp_id),
                    found_by="DblpMatching::VenueInformation",
                )
            return NameMatch(
                event=wiki_event,
                series=has_part.part.name,
                found_by="DblpMatching::VenueInformation",
            )
        return None

    def _find_series_for_conference(
        self, event: DblpEvent, scraper: DblpScraper
    ) -> Optional[Match]:
        possible_parents: List[Dict[str, str]] = scraper.extract_parents_from_dblp_event_id(
            event.dblp_id
        )
        possible_parent_ids: List[str] = [parent["dblp_id"] for parent in possible_parents]
        if len(possible_parent_ids) == 0:
            logging.warning("Could not find any parents in dblp for conference: %s", event)
            return None
        if len(possible_parent_ids) > 0:
            logging.info(
                "Found multiple possible series (%s) for conference (%s).", possible_parents, event
            )
        parent: str = (
            possible_parent_ids[0]
            if len(possible_parent_ids) == 1
            else get_next_or_none(possible_parent_ids, event.dblp_id.startswith)
        )
        # Make sure the parent is not a workshop.
        if self._series_contains_workshop_in_title(
            parent
        ) and not self._series_contains_conference_in_title(parent):
            logging.warning(
                "Found series (%s) for conference (%s) that is a workshop.", parent, event.dblp_id
            )
            return None
        return DblpMatch(
            event=self.dbpl_to_wikidata[event],
            series=self.repo.get_dblp_event_series_by_id(parent),
            found_by="DblpMatcher::_find_series_for_conference",
        )

    def _series_contains_workshop_in_title(self, parent_dblp_id: str):
        return self._series_title_contains(parent_dblp_id, "workshop")

    def _series_contains_conference_in_title(self, parent_dblp_id: str):
        return self._series_title_contains(parent_dblp_id, "conference")

    def _series_title_contains(self, series_id: str, target: str):
        series: DblpEventSeries = self.repo.get_dblp_event_series_by_id(dblp_id=series_id)
        return target in series.name.lower()
