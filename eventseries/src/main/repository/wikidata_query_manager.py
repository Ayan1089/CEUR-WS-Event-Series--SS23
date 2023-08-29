from importlib import resources as ires
from pathlib import Path
from typing import List, Dict

from lodstorage.sparql import SPARQL

from eventseries.src.main.repository.cached_online_context import CachedContext, T


class WikiDataQueryManager(CachedContext[List[Dict]]):
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
        build_dict[file_path.stem] = CachedContext._load_json(file_path)

    def store_content_to_file(self, file_path, file_content, overwrite: bool):
        CachedContext._store_json(file_content, file_path, overwrite)

    def exec_query(self, query_id: str, query_string: str, ignore_cache: bool = False):
        if ignore_cache or not self.is_cached(query_id):
            lod = self.sparql.queryAsListOfDicts(queryString=query_string)
            self.cache_content(query_id, lod)
        return self.get_cached(query_id)

    def _wikidata_events(self, query_name: str, filter_line: str = "", **kwargs) -> List[Dict]:
        query = """
        SELECT DISTINCT ?event ?eventLabel
            (SAMPLE(?_title) as ?title)
            (SAMPLE(?_acronym) as ?acronym)
            (SAMPLE(?_startTime) as ?startTime)
            (SAMPLE(?_endTime) as ?endTime)
            (SAMPLE(?_country) as ?country)
            (SAMPLE(?_location) as ?location)
            (SAMPLE(?_officialWebsite) as ?officialWebsite)
            (SAMPLE(?_colocatedWith) as ?colocatedWith)
            (SAMPLE(?_dblpEventId) as ?dblpEventId)
            (SAMPLE(?_wikiCfpId) as ?wikiCfpId)
            (SAMPLE(?_series) as ?series)
            (SAMPLE(?_ordinal) as ?ordinal)
            (SAMPLE(?_ceurwsUrl) as ?ceurwsUrl)
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

        return self.exec_query(query_name, query, **kwargs)

    def wikidata_all_ceurws_events(self) -> List[Dict]:
        return self._wikidata_events("wikidata_all_ceurws_events")

    def wikidata_conferences(self) -> List[Dict]:
        return self._wikidata_events(
            "wikidata_conferences",
            filter_line="""FILTER((CONTAINS(LCASE(?_title),"conference")
                  || EXISTS{?event wdt:P31 wd:Q2020153.})
                 && !CONTAINS(LCASE(?_title),"workshop"))""",
        )

    def wikidata_workshops(self) -> List[Dict]:
        return self._wikidata_events(
            "wikidata_workshops",
            filter_line="""FILTER(CONTAINS(LCASE(?_title),"workshop")
              || EXISTS{?event wdt:P31 wd:Q40444998.})""",
        )

    def _wikidata_event_series(
        self, query_name: str, filter_line: str = "", **kwargs
    ) -> List[Dict]:
        query = """
        SELECT DISTINCT ?series ?seriesLabel
          (SAMPLE(?_title) as ?title)
          (SAMPLE(?_acronym) as ?acronym)
          (SAMPLE(?_officialWebsite) as ?officialWebsite)
          (GROUP_CONCAT(?_instanceOf) as ?instanceOf) 
          (SAMPLE(?_dblpVenueId) as ?dblpVenueId)
          (SAMPLE(?_wikiCfpSeriesId) as ?wikiCfpSeriesId)
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
        return self.exec_query(query_name, query, **kwargs)

    def wikidata_all_ceurws_event_series(self, **kwargs) -> List[Dict]:
        return self._wikidata_event_series("wikidata_all_ceurws_event_series", **kwargs)

    def wikidata_conference_series(self, **kwargs) -> List[Dict]:
        return self._wikidata_event_series(
            query_name="wikidata_conference_series",
            filter_line="""FILTER((CONTAINS(LCASE(?_title),"conference")
          || EXISTS{?series wdt:P31 wd:Q47258130.})
         && !CONTAINS(LCASE(?_title),"workshop"))""",
            **kwargs
        )

    def wikidata_workshop_series(self, **kwargs) -> List[Dict]:
        return self._wikidata_event_series(
            query_name="wikidata_workshop_series",
            filter_line="""FILTER(CONTAINS(LCASE(?_title),"workshop")
          || EXISTS{?series wdt:P31 wd:Q47459256.})""",
            **kwargs
        )

    def wikidata_all_proceedings(self, **kwargs) -> List[Dict]:
        query = """
        SELECT DISTINCT ?event ?proceeding 
          (SAMPLE(?_title) as ?title)
          (SAMPLE(?_proceedingTitle) as ?proceedingTitle)
          (SAMPLE(?_acronym) as ?acronym) 
          (SAMPLE(?_startTime) as ?startTime) 
          (SAMPLE(?_endTime) as ?endTime)
          (SAMPLE(?_country) as ?country) 
          (SAMPLE(?_location) as ?location) 
          (SAMPLE(?_officialWebsite) as ?officialWebsite) 
          (SAMPLE(?_colocatedWith) as ?colocatedWith) 
          (SAMPLE(?_dblpEventId) as ?dblpEventId) 
          (SAMPLE(?_wikiCfpId) as ?wikiCfpId) 
          (SAMPLE(?_series) as ?series) 
          (SAMPLE(?_ordinal) as ?ordinal)
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
          OPTIONAL{?event wdt:P11633 ?_colocatedWith.} 
          OPTIONAL{?event wdt:P10692 ?_dblpEventId.} 
          OPTIONAL{?event wdt:P5124 ?_wikiCfpId.}
          OPTIONAL{?event wdt:P179 ?_series.} 
          OPTIONAL{?event p:P179/pq:P1545 ?_ordinal.}
          OPTIONAL{?proceeding wdt:P1476 ?_proceedingTitle. FILTER(lang(?_proceedingTitle)="en")}
        }
        GROUP BY ?event ?proceeding
        ORDER BY DESC(?startTime)
        """

        return self.exec_query("wikidata_all_proceedings", query, **kwargs)
