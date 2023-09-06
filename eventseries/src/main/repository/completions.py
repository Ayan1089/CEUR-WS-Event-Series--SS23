import abc
import dataclasses
import json
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Union, Tuple

from eventseries.src.main.dblp.event_classes import DblpEventSeries
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEventSeries,
    WikiDataEvent,
    QID,
    WikiDataEventType,
    WikiDataProceeding,
)


@dataclass
class Match:
    event: WikiDataEvent
    found_by: str
    series: Union[WikiDataEventSeries, DblpEventSeries, str]


@dataclass
class FullMatch(Match):
    series: WikiDataEventSeries


@dataclass
class NameMatch(Match):
    series: str


@dataclass
class DblpMatch(Match):
    series: DblpEventSeries


def get_titles_from_match(match: Match) -> Tuple[str, str]:
    """
    Return the titles of the event and series.
    :param match: A FullMatch, NameMatch or DBlpMatch of which the titles should be extracted.
    :return: A tuple of (event_title,series_title).
    """
    event_title = match.event.title
    series_title = ""
    if isinstance(match, FullMatch):
        series_title = match.series.title if match.series.title is not None else match.series.label
    if isinstance(match, DblpMatch):
        series_title = match.series.name
    if isinstance(match, NameMatch):
        series_title = match.series
    if series_title == "":
        raise ValueError("Could not get title of series from match: " + str(match))
    return event_title, series_title


@dataclass
class WikidataItemCompletion:
    qid: QID
    found_by: Optional[str]

    @abc.abstractmethod
    def patch_item(self, wiki_item: Union[WikiDataEvent, WikiDataEventSeries, WikiDataProceeding]):
        pass


@dataclass
class WithOrdinal(WikidataItemCompletion):
    ordinal: int

    def patch_item(self, wiki_item: WikiDataEvent):
        wiki_item.ordinal = self.ordinal


@dataclass
class WithVolume(WikidataItemCompletion):
    volume: int

    def patch_item(self, wiki_item: WikiDataProceeding):
        wiki_item.volume = self.volume


@dataclass
class WithAcronym(WikidataItemCompletion):
    acronym: str

    def patch_item(self, wiki_item: Union[WikiDataEvent, WikiDataEventSeries, WikiDataProceeding]):
        wiki_item.acronym = self.acronym


@dataclass
class WithCeurWsTitle(WikidataItemCompletion):
    ceurws_title: str

    def patch_item(self, wiki_item: WikiDataProceeding):
        wiki_item.ceurws_title = self.ceurws_title


@dataclass
class WithItemType(WikidataItemCompletion):
    event_type: WikiDataEventType

    def patch_item(self, wiki_item: Union[WikiDataEvent, WikiDataEventSeries]):
        wiki_item.type = WikiDataEventType


class EnhancedJSONEncoder(json.JSONEncoder):  # https://stackoverflow.com/a/51286749
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)
