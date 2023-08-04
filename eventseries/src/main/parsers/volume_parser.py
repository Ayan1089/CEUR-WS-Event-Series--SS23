import concurrent.futures
import os

import requests
from bs4 import BeautifulSoup

from eventseries.src.main.util.cache import Cache
from eventseries.src.main.util.record_attributes import CEUR_WS_TITLE, CEUR_SPT_URL
from eventseries.src.main.util.utility import Utility


class VolumeParser:
    cache = None

    def __init__(self):
        self.cache = Cache()

    def parse_ceur_ws_title(self, records: list) -> list:
        records_with_titles = []

        # Fetch the titles from the cache
        self.cache.load_cache(
            os.path.join(os.path.abspath("resources"), "ceurws_title.pickle")
        )

        # Function to extract the title from a URL
        def extract_title(record: dict) -> dict:
            util = Utility()
            if not self.cache.is_empty():
                if self.cache.get(
                    util.extract_vol_number(
                        "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-",
                        record[CEUR_SPT_URL],
                    )
                ):
                    record[CEUR_WS_TITLE] = self.cache.get(
                        util.extract_vol_number(
                            "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-",
                            record[CEUR_SPT_URL],
                        )
                    )
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
                    self.cache.set(
                        util.extract_vol_number(
                            "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-",
                            record[CEUR_SPT_URL],
                        ),
                        response.json()["cvb.voltitle"],
                    )
                    record[CEUR_WS_TITLE] = response.json()["cvb.voltitle"]
                    return record
                else:
                    if (
                        "cvb.title" in response.json()
                        and response.json()["cvb.title"] is not None
                        and len(response.json()["cvb.title"]) != 0
                    ):
                        self.cache.set(
                            util.extract_vol_number(
                                "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-",
                                record[CEUR_SPT_URL],
                            ),
                            response.json()["cvb.title"],
                        )
                        record[CEUR_WS_TITLE] = response.json()["cvb.title"]
                        return record
                    else:
                        print(
                            "volume title absent from ceurspt url: "
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
                if title[CEUR_WS_TITLE] is not None:
                    records_with_titles.append(title)

        self.cache.save_cache(
            os.path.join(os.path.abspath("resources"), "ceurws_title.pickle")
        )
        self.cache.print_cache_data(
            os.path.join(os.path.abspath("resources"), "ceurws_title.pickle")
        )
        return records_with_titles

    def extract_title_ceurws_url(self, url):
        self.cache.load_cache(
            os.path.join(os.path.abspath("resources"), "ceurws_title.pickle")
        )
        util = Utility()
        url = util.generate_ceurws_url(url)
        # print("########Searching in CEUR-WS url########" + url)
        soup = BeautifulSoup(requests.get(url).content, "html.parser")
        tag = soup.find("span", class_="CEURVOLTITLE")
        if tag:
            self.cache.set(
                util.extract_vol_number("https://ceur-ws.org/Vol-", url), tag.get_text()
            )
            return tag.get_text()
        else:
            print("########Vol title not in CEUR-WS url########" + url)
        return
