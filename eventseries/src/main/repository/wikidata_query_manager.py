from importlib import resources as ires
from pathlib import Path
from typing import List, Dict, Callable, Optional, Union

from lodstorage.sparql import SPARQL

from eventseries.src.main.repository.cached_online_context import CachedContext, T
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataEventType,
    WikiDataEventSeries,
    WikiDataProceeding,
    QID,
)


def _rename_attribute(old_name: str, new_name: str, items: List[Dict]):
    for item in items:
        if old_name in item:
            item[new_name] = item[old_name]
            del item[old_name]


def parse_qid(qid_string: str) -> QID:
    if qid_string.startswith("http://www.wikidata.org/entity/"):
        return QID(value=qid_string.removeprefix("http://www.wikidata.org/entity/"))
    return QID(value=qid_string)


def _parse_attributes(attribute_name: str, parsing_func: Callable, items: List[Dict]):
    for item in items:
        if attribute_name in item:
            item[attribute_name] = parsing_func(item[attribute_name])


def _parse_all_events(results: List[Dict]):
    _rename_attribute("eventLabel", "label", results)
    _rename_attribute("event", "qid", results)
    _parse_attributes("qid", parse_qid, results)
    _parse_attributes("part_of_series", parse_qid, results)
    _parse_attributes("country", parse_qid, results)
    _parse_attributes("location", parse_qid, results)
    _parse_attributes("colocated_with", parse_qid, results)
    return [WikiDataEvent(**event) for event in results]


def _parse_series(results: List[Dict]) -> List[WikiDataEventSeries]:
    for series in results:
        instances = list(map(parse_qid, set(series["instance_of"].split(" "))))
        series["instance_of"] = instances
    _rename_attribute("seriesLabel", "label", results)
    _rename_attribute("series", "qid", results)
    _parse_attributes("qid", parse_qid, results)
    return [WikiDataEventSeries(**event) for event in results]


def _parse_proceedings(results: List[Dict]) -> List[WikiDataProceeding]:
    _rename_attribute("proceedingLabel", "label", results)
    _rename_attribute("proceeding", "qid", results)
    _parse_attributes("event", parse_qid, results)
    _parse_attributes("qid", parse_qid, results)
    return [WikiDataProceeding(**proceeding) for proceeding in results]


def _add_is_conference(items: Union[List[WikiDataEvent], List[WikiDataEventSeries]]):
    for conference in items:
        conference.type = WikiDataEventType.CONFERENCE
    return items


def _add_is_workshop(items: Union[List[WikiDataEvent], List[WikiDataEventSeries]]):
    for workshop in items:
        workshop.type = WikiDataEventType.WORKSHOP
    return items


class WikiDataQueryManager(CachedContext[List]):
    WORKSHOPS = "WORKSHOPS"
    CONFERENCES = "CONFERENCES"
    EVENTS = "EVENTS"
    SERIES = "SERIES"
    WORKSHOP_SERIES = "WORKSHOP_SERIES"
    CONFERENCE_SERIES = "CONFERENCE_SERIES"
    PROCEEDINGS = "PROCEEDINGS"

    def __init__(
        self,
        url: str = "https://query.wikidata.org/sparql",
        resource_dir: Path = ires.files("eventseries.src.main") / "resources" / "query_results",
        load_on_init: bool = True,
        store_on_delete: bool = True,
    ):
        super().__init__(resource_dir, load_on_init, store_on_delete)
        self.url = url
        self.sparql = SPARQL(url=url)

    def load_cached_file(self, build_dict: Dict[str, T], file_path: Path):
        if file_path.stem in (
                WikiDataQueryManager.WORKSHOPS,
                WikiDataQueryManager.CONFERENCES,
                WikiDataQueryManager.EVENTS,
                WikiDataQueryManager.SERIES,
                WikiDataQueryManager.WORKSHOP_SERIES,
                WikiDataQueryManager.CONFERENCE_SERIES,
                WikiDataQueryManager.PROCEEDINGS,
        ):
            build_dict[file_path.stem] = CachedContext._load_pickle(file_path)
        else:
            build_dict[file_path.stem] = CachedContext._load_json(file_path)

    def store_content_to_file(self, file_path: Path, file_content, overwrite: bool):
        if file_path.stem in (
                WikiDataQueryManager.WORKSHOPS,
                WikiDataQueryManager.CONFERENCES,
                WikiDataQueryManager.EVENTS,
                WikiDataQueryManager.SERIES,
                WikiDataQueryManager.WORKSHOP_SERIES,
                WikiDataQueryManager.CONFERENCE_SERIES,
                WikiDataQueryManager.PROCEEDINGS,
        ):
            CachedContext._store_pickle(file_path, file_content, overwrite)
        else:
            CachedContext._store_json(file_content, file_path, overwrite)

    def exec_query(
            self,
            query_id: str,
            query_string: str,
            ignore_cache: bool = False,
            result_parser: Optional[Callable] = None,
    ):
        if ignore_cache or not self.is_cached(query_id):
            lod = self.sparql.queryAsListOfDicts(queryString=query_string)
            if result_parser:
                self.cache_content(query_id, result_parser(lod))
        return self.get_cached(query_id)

    def _wikidata_events(
            self,
            query_name: str,
            filter_line: str = "",
            result_parser: Callable = _parse_all_events,
            **kwargs
    ) -> List[WikiDataEvent]:
        query = """
            SELECT DISTINCT ?event ?eventLabel
                (SAMPLE(?_title) as ?title)
                (SAMPLE(?_acronym) as ?acronym)
                (SAMPLE(?_startTime) as ?start_time)
                (SAMPLE(?_endTime) as ?end_time)
                (SAMPLE(?_country) as ?country)
                (SAMPLE(?_location) as ?location)
                (SAMPLE(?_officialWebsite) as ?official_website)
                (SAMPLE(?_colocatedWith) as ?colocated_with)
                (SAMPLE(?_dblpEventId) as ?dblp_id)
                (SAMPLE(?_wikiCfpId) as ?wikicfp_id)
                (SAMPLE(?_series) as ?part_of_series)
                (SAMPLE(?_ordinal) as ?ordinal)
                (SAMPLE(?_ceurwsUrl) as ?ceurws_url)
            WHERE{
                ?proceeding wdt:P31 wd:Q1143604.
                ?proceeding wdt:P179 wd:Q27230297.
                ?proceeding p:P179/pq:P478 ?volumeNumber.
                ?proceeding wdt:P4745 ?event.
                OPTIONAL{?event wdt:P1476 ?_title. Filter(lang(?_title)="en")}
                OPTIONAL{?event wdt:P580 ?_startTime.}
                OPTIONAL{?event wdt:P582 ?_endTime.}
                OPTIONAL{?event wdt:P17 ?_country.}
                OPTIONAL{?event wdt:location ?_location.}
                OPTIONAL{?event wdt:P856 ?_officialWebsite.}
                OPTIONAL{?event wdt:P31 ?_instanceOf.}
                OPTIONAL{?event wdt:P1813 ?_acronym.}
                OPTIONAL{?proceeding wdt:P973 ?_ceurwsUrl.}
                OPTIONAL{?event wdt:P11633 ?_colocatedWith.}
                OPTIONAL{?event wdt:P10692 ?_dblpEventId.}
                OPTIONAL{?event wdt:P5124 ?_wikiCfpId.}
                OPTIONAL{?event wdt:P179 ?_series.}
                OPTIONAL{?event p:P179/pq:P1545 ?_ordinal.}
                SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                %s
            }
            GROUP BY ?event ?eventLabel""" % filter_line

        return self.exec_query(query_name, query, result_parser=result_parser, **kwargs)

    def wikidata_all_ceurws_events(self) -> List[WikiDataEvent]:
        return self._wikidata_events(WikiDataQueryManager.EVENTS)

    def wikidata_conferences(self) -> List[WikiDataEvent]:
        return self._wikidata_events(
            WikiDataQueryManager.CONFERENCES,
            filter_line="""FILTER((CONTAINS(LCASE(?_title),"conference")
                      || EXISTS{?event wdt:P31 wd:Q2020153.})
                     && !CONTAINS(LCASE(?_title),"workshop"))""",
            result_parser=lambda results: _add_is_conference(_parse_all_events(results))
        )

    def wikidata_workshops(self) -> List[WikiDataEvent]:

        return self._wikidata_events(
            WikiDataQueryManager.WORKSHOPS,
            filter_line="""FILTER(CONTAINS(LCASE(?_title),"workshop")
                  || EXISTS{?event wdt:P31 wd:Q40444998.})""",
            result_parser=lambda results: _add_is_workshop(_parse_all_events(results)),
        )

    def _wikidata_event_series(
            self,
            query_name: str,
            filter_line: str = "",
            result_parser: Callable = _parse_series,
            **kwargs
    ) -> list[WikiDataEventSeries]:
        query = """
            SELECT DISTINCT ?series ?seriesLabel
              (SAMPLE(?_title) as ?title)
              (SAMPLE(?_acronym) as ?acronym)
              (SAMPLE(?_officialWebsite) as ?official_website)
              (GROUP_CONCAT(?_instanceOf) as ?instance_of) 
              (SAMPLE(?_dblpVenueId) as ?dblp_id)
              (SAMPLE(?_wikiCfpSeriesId) as ?wikicfp_id)
            WHERE{
              ?proceeding wdt:P31 wd:Q1143604.
              ?proceeding wdt:P179 wd:Q27230297.
              ?proceeding p:P179/pq:P478 ?volumeNumber.
              ?proceeding wdt:P4745 ?event.
              ?event wdt:P179 ?series.
              OPTIONAL{?series wdt:P1476 ?_title. Filter(lang(?_title)="en")}
              OPTIONAL{?series wdt:P856 ?_officialWebsite.}
              OPTIONAL{?series wdt:P31 ?_instanceOf.}
              OPTIONAL{?series wdt:P1813 ?_acronym.}
              OPTIONAL{?series wdt:P8926 ?_dblpVenueId.} 
              OPTIONAL{?series wdt:P5127 ?_wikiCfpSeriesId.}
              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
              %s
            }
            GROUP BY ?series ?seriesLabel
        """ % filter_line

        return self.exec_query(query_name, query, result_parser=result_parser, **kwargs)

    def wikidata_all_ceurws_event_series(self, **kwargs) -> List[WikiDataEventSeries]:
        return self._wikidata_event_series(WikiDataQueryManager.SERIES, **kwargs)

    def wikidata_conference_series(self, **kwargs) -> List[WikiDataEventSeries]:
        return self._wikidata_event_series(
            query_name=WikiDataQueryManager.CONFERENCE_SERIES,
            filter_line="""FILTER((CONTAINS(LCASE(?_title),"conference")
              || EXISTS{?series wdt:P31 wd:Q47258130.})
             && !CONTAINS(LCASE(?_title),"workshop"))""",
            result_parser=lambda results: _add_is_conference(_parse_series(results)),
            **kwargs,
        )

    def wikidata_workshop_series(self, **kwargs) -> List[WikiDataEventSeries]:
        return self._wikidata_event_series(
            query_name=WikiDataQueryManager.WORKSHOP_SERIES,
            filter_line="""FILTER(CONTAINS(LCASE(?_title),"workshop")
              || EXISTS{?series wdt:P31 wd:Q47459256.})""",
            result_parser=lambda results: _add_is_workshop(_parse_series(results)),
            **kwargs,
        )

    def wikidata_all_proceedings(self, **kwargs) -> List[WikiDataProceeding]:

        query = """
            SELECT DISTINCT ?proceeding ?proceedingLabel ?event
              (SAMPLE(?_short_name) as ?acronym)
              (SAMPLE(?_pubDate) as ?pub_date)
              (SAMPLE(?_ceurwsUrl) as ?ceurws_url)
              (SAMPLE(?_described_at_url) as ?described_at_url)
              (SAMPLE(?_dblpPublicationId) as ?dblp_id)
              (SAMPLE(?title) as ?title)
              (SAMPLE(?volume_number) as ?volume_number)
            WHERE{
              ?proceeding wdt:P31 wd:Q1143604.
              ?proceeding wdt:P179 wd:Q27230297.
              ?proceeding p:P179/pq:P478 ?volume_number.
              ?proceeding wdt:P4745 ?event.
              ?proceeding wdt:P1476 ?title. Filter(lang(?title)="en")
              OPTIONAL{?proceeding wdt:P1813 ?_short_name.}
              OPTIONAL{?proceeding wdt:P577 ?_pubDate.}
              OPTIONAL{?proceeding wdt:P973 ?_ceurwsUrl.}
              OPTIONAL{?proceeding wdt:973 ?described_at_url.}
              OPTIONAL{?proceeding wdt:P8978 ?_dblpPublicationId.}
              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            } 
            GROUP BY ?proceeding ?proceedingLabel ?event
            """

    return self.exec_query(
        WikiDataQueryManager.PROCEEDINGS, query, result_parser=_parse_proceedings, **kwargs
    )
