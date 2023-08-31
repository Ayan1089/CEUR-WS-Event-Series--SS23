import abc
import dataclasses
import json
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, Optional

from easydict import EasyDict


@dataclass
class Match:
    series: Dict
    event: Dict
    found_by: str


@dataclass
class WikidataEventCompletion:
    qid: str
    found_by: Optional[str]

    @abc.abstractmethod
    def patch_event(self, event: EasyDict):
        pass


@dataclass
class WithOrdinal(WikidataEventCompletion):
    ordinal: int

    def patch_event(self, event: EasyDict):
        event.ordinal = self.ordinal


@dataclass
class WithVolume(WikidataEventCompletion):
    volume: int

    def patch_event(self, event: EasyDict):
        event.volume = self.volume


@dataclass
class WithAcronym(WikidataEventCompletion):
    acronym: str

    def patch_event(self, event: EasyDict):
        event.acronym = self.acronym


@dataclass
class WithCeurWsTitle(WikidataEventCompletion):
    ceurws_title: str

    def patch_event(self, event: EasyDict):
        event.ceurws_title = self.ceurws_title


@dataclass
class WithEventType(WikidataEventCompletion):
    is_workshop: bool
    is_conference: bool

    def patch_event(self, event: EasyDict):
        event.is_workshop = self.is_workshop
        event.is_conference = self.is_conference


class EnhancedJSONEncoder(json.JSONEncoder):  # https://stackoverflow.com/a/51286749
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)
