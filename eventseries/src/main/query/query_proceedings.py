import os

import requests
from lodstorage.query import Query
from lodstorage.sparql import SPARQL


class WikidataEventsProceedings(object):
    def read_as_dict(self):
        query_record = {
            "lang": "sparql",
            "name": "CEUR-WS events",
            "title": "Extracting all CEUR-WS events",
            "description": "Wikidata SPARQL query showing the CEUR-WS events",
            "query": """SELECT DISTINCT ?event ?proceeding 
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
                    """,
        }
        endpoint_url = "https://query.wikidata.org/sparql"
        endpoint = SPARQL(endpoint_url)
        query = Query(**query_record)
        qlod = endpoint.queryAsListOfDicts(query.query)
        return qlod
