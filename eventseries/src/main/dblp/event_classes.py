from dataclasses import dataclass
from typing import List, Optional

from eventseries.src.main.dblp.venue_information import VenueInformation


@dataclass(frozen=True)
class Event:
    """An event that is mentioned in a EventSeries."""

    title: str
    year: Optional[int]
    location: Optional[str]
    ordinal: Optional[str]


@dataclass(eq=True, frozen=True)
class DblpEvent(Event):
    dblp_id: str

    def __hash__(self) -> int:
        return hash(self.dblp_id)


@dataclass(eq=True, frozen=True)
class DblpEventSeries:
    dblp_id: str
    name: str
    abbreviation: Optional[str]
    venue_information: Optional[VenueInformation]
    mentioned_events: List[Event]

    def __hash__(self) -> int:
        return hash(self.dblp_id)
