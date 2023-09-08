from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional

import regex as re


@dataclass(eq=True, frozen=True)
class QID:
    value: str

    def __post_init__(self):
        if not isinstance(self.value, str):
            raise TypeError("Expected a string for: " + str(self.value))
        if re.match(r"Q\d+", self.value) is None:
            raise ValueError("Expected the qid to be of the form Qd+")


# Used both for events and series
class WikiDataEventType(Enum):
    WORKSHOP = "WORKSHOP"
    CONFERENCE = "CONFERENCE"
    UNKNOWN = "UNKNOWN"


# Would be better to use inheritance but dataclass-inheritance with default values are problematic.


@dataclass
class WikiDataEvent:
    qid: QID
    label: str
    title: Optional[str] = None
    acronym: Optional[str] = None
    dblp_id: Optional[str] = None
    ordinal: Optional[int] = None
    part_of_series: Optional[QID] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    country: Optional[QID] = None
    location: Optional[QID] = None
    official_website: Optional[str] = None
    colocated_with: Optional[QID] = None
    wikicfp_id: Optional[str] = None
    type: WikiDataEventType = WikiDataEventType.UNKNOWN
    ceurws_url: Optional[str] = None
    described_at_url: Optional[str] = None


@dataclass
class WikiDataEventSeries:
    qid: QID
    label: str
    title: Optional[str] = None
    acronym: Optional[str] = None
    dblp_id: Optional[str] = None
    official_website: Optional[str] = None
    wikicfp_id: Optional[str] = None
    instance_of: List[QID] = field(default_factory=lambda: [])
    type: WikiDataEventType = WikiDataEventType.UNKNOWN


def get_title_else_label(item) -> str:
    if hasattr(item, "title") and item.title is not None:
        return item.title
    if hasattr(item, "label") and item.label is not None:
        return item.label
    raise ValueError("Argument had neither title nor label: " + str(item))


@dataclass
class WikiDataProceeding:
    volume_number: int
    event: QID
    qid: QID
    label: str
    title: Optional[str] = None
    ceurws_title: Optional[str] = None
    pub_date: Optional[datetime] = None
    ceurws_url: Optional[str] = None
    described_at_url: Optional[str] = None
    acronym: Optional[str] = None
    dblp_id: Optional[str] = None
