import os

import requests
from lodstorage.query import Query
from lodstorage.sparql import SPARQL


class Events(object):
    def query(self):
        url = "https://query.wikidata.org/sparql"
        query = """SELECT DISTINCT ?event 
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
            }
            GROUP BY ?event
            ORDER BY DESC(?startTime)
            """
        params = {"query": query, "format": "json"}  # or json
        response = requests.request("POST", url, params=params)

        resources_path = os.path.abspath("resources")
        file = open(os.path.join(resources_path, "events.json"), "w", encoding="utf-8")
        file.write(response.text)
        file.close()

    """TODO: correct this, 2 queries are not required"""

    def read_as_dict(self):
        query_record = {
            "lang": "sparql",
            "name": "CEUR-WS events",
            "title": "Extracting all CEUR-WS events",
            "description": "Wikidata SPARQL query showing the CEUR-WS events",
            "query": """SELECT DISTINCT ?event ?eventLabel
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
                          (SAMPLE(?_volumeNumber) as ?volumeNumber)
                          (SAMPLE(?_ceurwsUrl) as ?ceurwsUrl)
                        WHERE{
                          ?proceeding wdt:P31 wd:Q1143604.
                          ?proceeding wdt:P179 wd:Q27230297.
                          ?proceeding wdt:P4745 ?event.
                          OPTIONAL{?proceeding p:P179/pq:P478 ?_volumeNumber.}
                          OPTIONAL{?proceeding wdt:P973 ?_ceurwsUrl.}
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
                          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                        }
                        GROUP BY ?event ?eventLabel
                        ORDER BY DESC(?startTime)
                    """,
        }
        endpoint_url = "https://query.wikidata.org/sparql"
        endpoint = SPARQL(endpoint_url)
        query = Query(**query_record)
        qlod = endpoint.queryAsListOfDicts(query.query)
        return qlod

    def event_series_query(self):
        url = "https://query.wikidata.org/sparql"
        query = """SELECT DISTINCT ?series ?seriesLabel
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
                }
                GROUP BY ?series ?seriesLabel
            """
        params = {"query": query, "format": "json"}  # or json
        response = requests.request("POST", url, params=params)

        resources_path = os.path.abspath("resources")
        file = open(
            os.path.join(resources_path, "event_series.json"), "w", encoding="utf-8"
        )
        file.write(response.text)
        file.close()
