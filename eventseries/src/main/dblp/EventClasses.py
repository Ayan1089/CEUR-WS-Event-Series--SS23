from dataclasses import dataclass
from typing import Optional, List

# from VenueInformationClasses import VenueInformation
from eventseries.src.main.dblp.VenueInformationClasses import VenueInformation


@dataclass
class Event:
    """An event that is mentioned in a EventSeries."""

    title: str
    year: Optional[int]
    location: Optional[str]
    ordinal: Optional[str]


@dataclass
class DblpEvent(Event):
    dblp_id: str


@dataclass
class EventSeries:
    dblp_id: str
    name: str
    abbreviation: Optional[str]
    venue_information: Optional[VenueInformation]
    mentioned_events: List[Event]

    def __hash__(self) -> int:
        return hash(self.dblp_id)
