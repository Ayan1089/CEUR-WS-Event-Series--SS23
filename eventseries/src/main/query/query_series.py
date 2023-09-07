import os
from pathlib import Path

import requests


class WikidataEventSeries(object):
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

        resources_path = Path(__file__).resolve().parent / ".." / "resources"
        # file = open(resources_path, "w", encoding="utf-8")
        file = open(
            os.path.join(resources_path, "event_series.json"), "w", encoding="utf-8"
        )
        file.write(response.text)
        file.close()
