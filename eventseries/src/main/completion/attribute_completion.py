import asyncio
import itertools
import logging
import re
from dataclasses import asdict
from typing import List, Optional, Union, Callable

from aiohttp import ClientSession, ClientResponse, TCPConnector
from bs4 import BeautifulSoup
from plp.ordinal import Ordinal

from eventseries.src.main.repository.completions import (
    WithOrdinal,
    WithCeurWsTitle,
    WithAcronym,
    WikidataItemCompletion,
)
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataProceeding,
    WikiDataEventSeries,
)


def complete_information(repo: Repository):
    events: List[WikiDataEvent] = list(repo.events_by_qid.values())
    proceedings: List[WikiDataProceeding] = list(repo.proceeding_by_qid.values())

    # try to extract the ordinal for events
    without_ordinal_completion = _filter_uncompleted(events, WithOrdinal, lambda e: e.ordinal, repo)
    logging.debug("Completing ordinals for %s events.", len(without_ordinal_completion))
    ordinal_completions = complete_ordinals(without_ordinal_completion)
    logging.info("Found %s new ordinals for events.", len(ordinal_completions))
    for ordinal_completion in ordinal_completions:
        repo.completion_cache.add_completion(ordinal_completion)

    complete_acronym_information(repo)

    # add ceurws_title to all proceedings
    without_ceurws_title_completion: List[WikiDataProceeding] = _filter_uncompleted(
        proceedings, WithCeurWsTitle, lambda p: p.ceurws_title, repo
    )
    logging.debug("Completing ceurws-title for %s proceedings.", len(proceedings))
    title_completions = complete_ceurws_titles(without_ceurws_title_completion)
    logging.info("Found %s new ceurws-titles for proceedings.", len(ordinal_completions))
    for ceurws_title_completion in title_completions:
        repo.completion_cache.add_completion(ceurws_title_completion)


def complete_acronym_information(repo: Repository):
    # add acronyms to events and event_series
    events_and_series = itertools.chain(
        repo.events_by_qid.values(), repo.event_series_by_qid.values()
    )

    without_acronym_match = _filter_uncompleted(
        events_and_series, WithAcronym, lambda item: item.acronym, repo
    )
    logging.debug("Completing acronyms for %s events/series.", len(without_acronym_match))
    acronym_completions = complete_acronyms(without_acronym_match)
    logging.info("Found %s new acronyms for events and series.", len(acronym_completions))
    for acronym_completion in acronym_completions:
        repo.completion_cache.add_completion(acronym_completion)


def _filter_uncompleted(
    to_be_filtered, completion_class, attribute_supplier: Callable, repo: Repository
) -> List:
    filtered = []
    for item in to_be_filtered:
        if not hasattr(item, "qid"):
            logging.error("Item had no qid %s", item)
            continue
        completions = repo.completion_cache.get_completions_for_qid(item.qid)
        if attribute_supplier(item) is None and _no_completion(completion_class, completions):
            filtered.append(item)
    return filtered


def _no_completion(completion_class, completions: List[WikidataItemCompletion]):
    return not any(isinstance(comp, completion_class) for comp in completions)


def complete_ordinals(events: List[WikiDataEvent]) -> List[WithOrdinal]:
    completed_ordinals = []
    for event in events:
        if event.ordinal is not None:
            continue
        # addParsedOrdinal expects a dictionary and will search for the ordinal in "title"
        as_dict = asdict(event)
        del as_dict["ordinal"]  # we have to manually delete this entry for addParsedOrdinal to work
        if event.title is None:
            as_dict["title"] = event.label
        Ordinal.addParsedOrdinal(as_dict)
        if "ordinal" in as_dict:
            completed_ordinals.append(
                WithOrdinal(qid=event.qid, ordinal=as_dict["ordinal"], found_by="complete_ordinals")
            )
    return completed_ordinals


def complete_acronyms(items: List[Union[WikiDataEvent, WikiDataEventSeries]]) -> List[WithAcronym]:
    completed_acronyms = []
    for item in items:
        if item.acronym is not None:
            continue
        label_acronym = extract_acronym(item.label)
        title_acronym = extract_acronym(item.title) if item.title is not None else None
        if label_acronym and title_acronym and label_acronym != title_acronym:
            logging.warning(
                "Found different acronyms in label (%s) and title (%s): %s %s",
                item.label,
                item.title,
                label_acronym,
                title_acronym,
            )
        acronym = label_acronym if label_acronym else title_acronym
        completed_acronyms.append(
            WithAcronym(qid=item.qid, acronym=acronym, found_by="complete_acronyms")
        )
    return completed_acronyms


def complete_ceurws_titles(proceedings: List[WikiDataProceeding]) -> List[WithCeurWsTitle]:
    """We use coroutines to speed up bulk request."""
    return list(filter(lambda x: x is not None, asyncio.run(_complete_ceurws_titles_async(proceedings))))


async def _complete_ceurws_titles_async(
    proceedings: List[WikiDataProceeding],
) -> List[WithCeurWsTitle]:
    connector = TCPConnector(limit=50, force_close=True)  # otherwise server will close connection
    async with ClientSession(connector=connector) as session:
        tasks = []
        for proceeding in proceedings:
            tasks.append(_ceurws_title_async(proceeding, session))
        return await asyncio.gather(*tasks)


async def _ceurws_title_async(
    proceeding: WikiDataProceeding, session: ClientSession
) -> Optional[WithCeurWsTitle]:
    title = await _title_from_ceurspt(proceeding.volume_number, session)
    if not title:
        title = await _title_from_ceurws_org(proceeding.volume_number, session)
    if not title:
        logging.info("Could not complete ceurws title for %s", proceeding)
        return None

    return WithCeurWsTitle(qid=proceeding.qid, found_by="complete_ceurws_title", ceurws_title=title)


async def _title_from_ceurspt(volume_number: int, session: ClientSession) -> Optional[str]:
    url = "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-" + str(volume_number) + ".json"
    opt_resp: Optional[ClientResponse] = await request_async(url=url, session=session)
    if not opt_resp:
        return None
    json_response = await opt_resp.json()
    if (
        "cvb.voltitle" in json_response
        and json_response["cvb.voltitle"] is not None
        and len(json_response["cvb.voltitle"]) != 0
    ):
        return json_response["cvb.voltitle"]

    if (
        "cvb.title" in json_response
        and json_response["cvb.title"] is not None
        and len(json_response["cvb.title"]) != 0
    ):
        return json_response["cvb.title"]
    return None


async def _title_from_ceurws_org(volume_number: int, session: ClientSession) -> Optional[str]:
    url = "https://ceur-ws.org/Vol-" + str(volume_number)
    response = await request_async(url=url, session=session)
    if not response:
        return None
    content = await response.read()
    soup = BeautifulSoup(content, "html.parser")
    tag = soup.find("span", class_="CEURVOLTITLE")
    if tag:
        return tag.get_text()
    return None


async def request_async(url: str, session: ClientSession, **kwargs) -> Optional[ClientResponse]:
    resp = await session.request(method="GET", url=url, **kwargs)
    if not resp.ok:
        return None
    return resp


def extract_acronym(input_string: str) -> Optional[str]:
    pattern = r"\((.*?)\)"
    matches = re.search(pattern, input_string)

    if matches:
        return matches.group(1)
    return None
