import concurrent.futures
import os
import threading
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from eventseries.src.main.util.cache import Cache
from eventseries.src.main.util.record_attributes import CEUR_WS_TITLE, CEUR_SPT_URL
from eventseries.src.main.util.utility import Utility


class VolumeParser:
    def __init__(self):
        self.cacheObj = Cache()

    def parse_ceur_ws_title(self, records: list) -> list:
        records_with_titles = []

        if __name__ == "__main__":
            resources_path = Path(__file__).resolve().parent / ".." / "resources"
        else:
            resources_path = (
                    Path(__file__).resolve().parent
                    / ".."
                    / ".."
                    / "tests"
                    / "query"
                    / "resources"
            )
        # Fetch the titles from the cache
        self.cacheObj.load_volume_cache(os.path.join(resources_path, "ceurws_title.pickle"))

        self.cacheObj.load_absent_volume_cache(os.path.join(resources_path, "ceurws_absent_volume_cache.pickle"))

        # Function to extract the title from a URL
        def extract_title(record: dict) -> dict:
            util = Utility()
            vol_number = util.extract_vol_number("http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-",
                                                 record[CEUR_SPT_URL])
            if self.cacheObj.volume_cache:
                if self.cacheObj.get(vol_number):
                    record[CEUR_WS_TITLE] = self.cacheObj.get(vol_number)
                    return record
                elif self.cacheObj.absent_volume_cache and vol_number in self.cacheObj.absent_volume_cache:
                    record[CEUR_SPT_URL] = None
                    return record
            response = requests.get(record[CEUR_SPT_URL])
            # soup = BeautifulSoup(response.content, 'html.parser')
            # tag = soup.find('span', class_='CEURVOLTITLE')
            if response.status_code == 200:
                if (
                        "cvb.voltitle" in response.json()
                        and response.json()["cvb.voltitle"] is not None
                        and len(response.json()["cvb.voltitle"]) != 0
                ):
                    record[CEUR_WS_TITLE] = response.json()["cvb.voltitle"]
                    return record
                else:
                    if (
                            "cvb.title" in response.json()
                            and response.json()["cvb.title"] is not None
                            and len(response.json()["cvb.title"]) != 0
                    ):
                        record[CEUR_WS_TITLE] = response.json()["cvb.title"]
                        return record
                    else:
                        print(
                            "Volume title absent from ceurspt url: "
                            + record[CEUR_SPT_URL]
                        )
                        record[CEUR_WS_TITLE] = self.extract_title_ceurws_url(
                            record[CEUR_SPT_URL]
                        )
                        return record

            else:
                print("ceurspt URL absent: " + record[CEUR_SPT_URL])
                record[CEUR_WS_TITLE] = self.extract_title_ceurws_url(
                    record[CEUR_SPT_URL]
                )
                return record

        # Create a ThreadPoolExecutor for parallel execution
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit the tasks for each record
            futures = [executor.submit(extract_title, record) for record in records]

            # Process the completed futures
            for future in concurrent.futures.as_completed(futures):
                title = future.result()
                if CEUR_WS_TITLE in title and title[CEUR_WS_TITLE] is not None:
                    records_with_titles.append(title)

        util = Utility()
        for record in records_with_titles:
            vol_number = util.extract_vol_number("http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-", record[CEUR_SPT_URL])
            self.cacheObj.volume_cache[vol_number] = record[CEUR_WS_TITLE]
            self.cacheObj.absent_volume_cache.append(vol_number)
        self.cacheObj.save_volume_cache(os.path.join(resources_path, "ceurws_title.pickle"))
        # self.cacheObj.print_cache_data(os.path.join(resources_path, "ceurws_title.pickle"))

        self.cacheObj.save_absent_volume_cache(os.path.join(resources_path, "ceurws_absent_volume_cache.pickle"))
        # self.cacheObj.print_cache_data(os.path.join(resources_path, "ceurws_title.pickle"))
        print()
        return records_with_titles

    def extract_title_ceurws_url(self, url):
        self.cacheObj.load_volume_cache(os.path.join(os.path.abspath("resources"), "ceurws_title.pickle"))
        util = Utility()
        url = util.generate_ceurws_url(url)
        vol_number = util.extract_vol_number("http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-", url)
        if vol_number in self.cacheObj.absent_volume_cache:
            print("Volume title not in CEUR-WS url: " + url)
        else:
            soup = BeautifulSoup(requests.get(url).content, "html.parser")
            tag = soup.find("span", class_="CEURVOLTITLE")
            if tag:
                return tag.get_text()
            else:
                print("Volume title not in CEUR-WS url: " + url)
            return
