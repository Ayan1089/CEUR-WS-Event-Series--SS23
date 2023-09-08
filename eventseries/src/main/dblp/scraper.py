import logging
from pathlib import Path
from typing import List, Optional, Tuple, Union, Dict

import bs4
from bs4 import BeautifulSoup, SoupStrainer
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from eventseries.src.main.dblp.dblp_context import (
    DblpContext,
    is_likely_dblp_event_series,
    get_dblp_id_from_url,
)
from eventseries.src.main.repository.repository import Repository


def scrape_wikidata_with_dblp_id(repo: Repository):
    scraper = DblpScraper(ctx=repo.dblp_repo.ctx)
    dblp_event_ids = [event.dblp_id for event in repo.events_by_qid.values() if event.dblp_id]
    scraper.crawl_events(dblp_event_ids)
    # load all DblpEvent and DblpEventSeries classes
    logging.debug("Successfully crawled %s events and their series.", len(dblp_event_ids))
    for series_id, events in repo.dblp_repo.ctx.get_series_with_events().items():
        repo.dblp_repo.get_or_load_event_series(series_id)
        for event_id in events:
            repo.dblp_repo.get_or_load_event(event_id)
    repo.dblp_repo.store_cached(overwrite=True)
    logging.debug("Parsed and stored DblpEvents and DblpEventSeries.")


class DblpScraper:
    def __init__(self, ctx: DblpContext) -> None:
        self.ctx: DblpContext = ctx

    def crawl_conf_index(
        self, index_url: str, pos: int = 0, driver_instance: Optional[WebDriver] = None
    ):
        """Crawl the index of conferences and workshops from dblp. Store results in cache."""
        full_url = index_url if pos == 0 else index_url + "?pos=" + str(pos)
        logging.info("Crawling everything from %s onward.", index_url)
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
        driver: WebDriver = driver_instance if driver_instance is not None else webdriver.Firefox()
        driver.get(conf_index_url)
        conferences_div = driver.find_element(By.id, "browse-conf-output")
        ul_elements: list[WebElement] = conferences_div.find_elements(By.TAG_NAME, "ul")
        a_elements: list[WebElement] = []
        for ul in ul_elements:
            li_elements = ul.find_elements(By.TAG_NAME, "li")
            for li in li_elements:
                a_elements.extend(li.find_elements(By.TAG_NAME, "a"))

        links = [a_ele.get_attribute("href") for a_ele in a_elements]

        next_page_link_list = [
            nextPage.get_attribute("href")
            for nextPage in conferences_div.find_element(By.TAG_NAME, "p").find_elements(
                By.TAG_NAME, "a"
            )
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
        soup = BeautifulSoup(content, "html.parser", parse_only=SoupStrainer("div", {"id": "main"}))
        real_url = (
            soup.find("div", {"id": "main"}).find("p", recursive=False).find("a").attrs["href"]
        )
        redirected_dblp_id = get_dblp_id_from_url(real_url)
        redirected_content = self.ctx.request_or_load_dblp(redirected_dblp_id)
        self.ctx.cache_dblp_id(dblp_id, redirected_content)

    def crawl_events(self, event_dblp_ids: List[str]) -> Dict[str, List[Dict]]:
        """
        1. Request or load the content of each event.
        2. Extract possible series which the event is part of.
        3. Request or load the parent series.
        4. Stores both in the dblp context of the scraper class.
        @:returns a dictionary mapping event dblp-ids to a dictionary {"dblp_id","name"}.
        """
        counter = 0
        logging.info("Crawling %s events.", len(event_dblp_ids))
        event_to_series: Dict[str, List[Dict]] = {}
        for dblp_id in event_dblp_ids:
            counter += 1
            try:
                html = self.ctx.request_or_load_dblp(dblp_db_entry=dblp_id, wait_time=1)
                self._resolve_redirecting(dblp_id, html)

            except ValueError as exc:
                logging.warning("Got exception for event: %s with error %s", dblp_id, exc)
            dblp_stem: str = str(Path(dblp_id).parent)
            parents: List[Dict] = self.extract_parents_from_dblp_event_id(event_id=dblp_id)
            if not any(parent["dblp_id"] == dblp_stem for parent in parents):
                logging.warning(
                    "Could not find stem of event id in breadcrumbs. Expected %s in %s for id %s.",
                    dblp_stem,
                    parents,
                    dblp_id
                )
            event_to_series[dblp_id] = parents

            try:
                for parent in parents:
                    self.ctx.request_or_load_dblp(dblp_db_entry=parent["dblp_id"], wait_time=1)
            except ValueError as exc:
                logging.warning("Got exception for event: %s with error %s.", dblp_stem, exc)
            if counter % 100 == 0:
                logging.info("Loaded: %s events.", counter)
            if counter % 200 == 0:
                self.ctx.store_cache(overwrite=True)
        return event_to_series

    def extract_parents_from_dblp_event_id(
        self, event_id: str, **kwargs: object
    ) -> List[Dict[str, str]]:
        """Extract parent-series information from the breadcrumbs section.
        :returns a list of parents each a dictionary containing dblp_id and name as keys.
        """
        if event_id.count("/") < 2:
            raise ValueError("Expected an event id but got" + str(event_id))
        event_content = self.ctx.request_or_load_dblp(dblp_db_entry=event_id, **kwargs)
        possible_parents = self.extract_parents_from_dblp_event_content(event_content=event_content)
        if not possible_parents:
            dblp_stem: str = "/".join(event_id.split("/")[:-1])  # remove the event from the id
            return [{"dblp_id": dblp_stem, "name": dblp_stem.rsplit("/", maxsplit=1)[-1]}]
        return possible_parents

    def extract_parents_from_dblp_event_content(
        self, event_content: Union[str, bs4.Tag]
    ) -> List[Dict[str, str]]:
        """
        :returns  a list of parents each a dictionary containing dblp_id and name as keys.
        """
        soup = (
            event_content
            if isinstance(event_content, bs4.Tag)
            else BeautifulSoup(
                event_content, "html.parser", parse_only=SoupStrainer("div", {"id": "breadcrumbs"})
            )
        )
        breadcrumbs = soup.find("div", {"id": "breadcrumbs"})
        parents_lists = breadcrumbs.find_all("li")
        direct_parents: List[Dict] = []
        for parent_li in parents_lists:
            spans = parent_li.find_all("span", itemprop="itemListElement")
            # Spans == 2 means we only have > Home > Conferences and Workshops
            if len(spans) == 2:
                continue
            direct_parent = self._get_span_with_highest_position(spans)
            a_tag: bs4.Tag = direct_parent.find("a", {"itemprop": "item"})
            dblp_id = get_dblp_id_from_url(a_tag["href"])
            name = a_tag.find("span").string
            if not is_likely_dblp_event_series(dblp_id):
                logging.warning("Extracted suspicious series id %s in tag %s", dblp_id, parent_li)
                continue
            direct_parents.append({"name": name, "dblp_id": dblp_id})

        return direct_parents

    def _get_span_with_highest_position(self, span_tags: List[bs4.Tag]) -> bs4.Tag:
        highest_position = -1
        highest_span = None
        for span in span_tags:
            position = int(span.find("meta", {"itemprop": "position"})["content"])
            if position > highest_position:
                highest_position = position
                highest_span = span
        return highest_span
