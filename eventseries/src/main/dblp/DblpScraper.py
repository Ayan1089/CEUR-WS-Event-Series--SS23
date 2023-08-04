import json
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd
from bs4 import SoupStrainer, BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from eventseries.src.main.dblp.DblpParsing import parse_venue_div
from eventseries.src.main.dblp.dblp import DblpContext


class DblpScraper:
    def __init__(self, ctx: DblpContext) -> None:
        self.ctx: DblpContext = ctx

    def extract_venue_information_of_cached(self):
        parse_restriction = SoupStrainer(id="info-section")
        soups = {
            dblp_id: BeautifulSoup(
                html, "html.parser", parse_only=parse_restriction
            ).find(id="info-section")
            for (dblp_id, html) in self.ctx.dblp_cache.items()
        }
        df = pd.DataFrame(soups.items(), columns=["dblp_id", "tag"])
        df = df[df.tag.notna()]  # filter out sites without venue information
        df = df[
            df.tag.apply(lambda div: len(div.find_all()) != 0)
        ]  # filter out emtpy divs
        df["venue"] = df["tag"].map(lambda div: parse_venue_div(div))
        return df

    def crawl_all_events_without_series(
        self,
        path_to_events: Path = Path("..") / "resources" / "EventsWithoutSeries.json",
    ):
        with open(path_to_events) as events_json_file:
            events_df = pd.DataFrame(json.loads(events_json_file.read()))
            with_dblp = events_df[events_df.dblpEventId.notna()]
            self.crawl_events(with_dblp.dblpEventId)

    def crawl_conf_index(
        self, index_url: str, pos: int = 0, driver_instance: Optional[WebDriver] = None
    ):
        full_url = index_url if pos == 0 else index_url + "?pos=" + str(pos)
        print("Crawling everything from " + index_url + " onward.")
        driver = webdriver.Firefox() if driver_instance is None else driver_instance
        a_elements, opt_next_link = DblpScraper.scrape_conf_index(
            conf_index_url=full_url, driver_instance=driver
        )
        self.resolve_and_load_conf_index_links(links=a_elements)
        self.ctx.store_cache()
        if opt_next_link is not None:
            self.crawl_conf_index(index_url=opt_next_link, driver_instance=driver)
        if driver_instance is None:
            driver.quit()

    @staticmethod
    def scrape_conf_index(
        conf_index_url: str, driver_instance: Optional[WebDriver] = None
    ) -> Tuple[List[str], Optional[str]]:
        driver: WebDriver = (
            driver_instance if driver_instance is not None else webdriver.Firefox()
        )
        driver.get(conf_index_url)
        conferences_div = driver.find_element(By.ID, "browse-conf-output")
        ul_elements: list[WebElement] = conferences_div.find_elements(By.TAG_NAME, "ul")
        a_elements: list[WebElement] = []
        for ul in ul_elements:
            li_elements = ul.find_elements(By.TAG_NAME, "li")
            for li in li_elements:
                a_elements.extend(li.find_elements(By.TAG_NAME, "a"))

        links = [a_ele.get_attribute("href") for a_ele in a_elements]

        next_page_link_list = [
            nextPage.get_attribute("href")
            for nextPage in conferences_div.find_element(
                By.TAG_NAME, "p"
            ).find_elements(By.TAG_NAME, "a")
            if nextPage.text == "[next 100 entries]"
        ]
        if len(next_page_link_list) > 0:
            next_page_link = next_page_link_list[0]
        else:
            next_page_link = None

        # Only quit if this method created the driver
        if driver_instance is None:
            driver.quit()
        # Don't close the window as this might quit the driver it is the only window.

        return links, next_page_link

    def resolve_and_load_conf_index_links(self, links: List[str]):
        for href in links:
            dblp_id = href.removeprefix(self.ctx.base_url)
            self.ctx.request_or_load_dblp(dblp_id, wait_time=1)

    def _resolve_redirecting(self, dblp_id: str, content: str):
        if "Redirecting ..." not in content:
            return
        soup = BeautifulSoup(
            content, "html.parser", parse_only=SoupStrainer("div", {"id": "main"})
        )
        real_url = (
            soup.find("div", {"id": "main"})
            .find("p", recursive=False)
            .find("a")
            .attrs["href"]
        )
        redirected_dblp_id = real_url.removeprefix("https://dblp.org/db/").removesuffix(
            "/index.html"
        )
        redirected_content = self.ctx.request_or_load_dblp(redirected_dblp_id)
        self.ctx.cache_dblp_id(dblp_id, redirected_content)

    def crawl_events(self, event_dblp_ids: List[str]):
        counter = 0
        print("Crawling " + str(len(event_dblp_ids)) + "...")
        for dblp_event in event_dblp_ids:
            counter += 1
            try:
                html = self.ctx.request_or_load_dblp(
                    dblp_db_entry=dblp_event, wait_time=1
                )
                self._resolve_redirecting(
                    dblp_event,
                    html,
                )

            except ValueError as e:
                print(
                    f"Got exception for event: " + dblp_event + " with error " + str(e)
                )
            parent = Path(dblp_event).parent
            try:
                self.ctx.request_or_load_dblp(dblp_db_entry=str(parent), wait_time=1)
            except ValueError as e:
                print(
                    f"Got exception for event: " + str(parent) + " with error " + str(e)
                )
            if counter % 100 == 0:
                print("Loaded: " + str(counter))
            if counter % 200 == 0:
                self.ctx.store_cache()
