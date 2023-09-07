import itertools
import logging
import re
from dataclasses import asdict
from typing import List, Optional, Union

import requests
from bs4 import BeautifulSoup
from plp.ordinal import Ordinal

from eventseries.src.main.repository.completions import WithOrdinal, WithCeurWsTitle, WithAcronym
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    WikiDataProceeding,
    QID,
    WikiDataEventSeries,
)


def complete_information(repo: Repository, ceurws_completion=False):
    events: List[WikiDataEvent] = list(repo.events_by_qid.values())
    proceedings: List[WikiDataProceeding] = list(repo.proceeding_by_event_qid.values())

    # try to extract the ordinal for events
    without_ordinal_completion = [
        event for event in events if _no_completion(event.qid, WithOrdinal, repo)
    ]
    for ordinal_completion in complete_ordinals(without_ordinal_completion):
        repo.completion_cache.add_completion(ordinal_completion)

    complete_acronym_information(repo)

    # add ceurws_title to all proceedings
    without_ceurws_title_completion = [
        proceeding
        for proceeding in proceedings
        if _no_completion(proceeding.qid, WithCeurWsTitle, repo)
    ]
    if ceurws_completion:
        for ceurws_title_completion in complete_ceurws_title(without_ceurws_title_completion):
            repo.completion_cache.add_completion(ceurws_title_completion)


def complete_acronym_information(repo: Repository):
    # add acronyms to events and event_series
    events_and_series = itertools.chain(
        repo.events_by_qid.values(), repo.event_series_by_qid.values()
    )

    without_acronym_match = list(
        filter(lambda item: _no_completion(item.qid, WithAcronym, repo), events_and_series)
    )

    acronym_completions = complete_acronyms(without_acronym_match)
    logging.info("Found %s new acronyms for events and series.", len(acronym_completions))
    for acronym_completion in acronym_completions:
        repo.completion_cache.add_completion(acronym_completion)


def _no_completion(qid: QID, completion_class, repo: Repository):
    return not any(
        isinstance(comp, completion_class)
        for comp in repo.completion_cache.get_completions_for_qid(qid)
    )


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


def complete_ceurws_title(proceedings: List[WikiDataProceeding]) -> List[WithCeurWsTitle]:
    completions = []
    for proceeding in proceedings:
        if proceeding.ceurws_title is not None:
            continue
        url = (
            "http://ceurspt.wikidata.dbis.rwth-aachen.de/Vol-"
            + str(proceeding.volume_number)
            + ".json"
        )
        response = requests.get(url=url, timeout=60)
        title = None
        if response.status_code == 200:
            title = _get_title_from_response(response.json())
        if title is None:
            title = _extract_title_from_ceurws_org(proceeding.volume_number)
        if title is not None:
            completions.append(
                WithCeurWsTitle(
                    qid=proceeding.qid, found_by="complete_ceurws_title", ceurws_title=title
                )
            )
    return completions


def _get_title_from_response(response_json: dict) -> Optional[str]:
    if (
        "cvb.voltitle" in response_json
        and response_json["cvb.voltitle"] is not None
        and len(response_json["cvb.voltitle"]) != 0
    ):
        return response_json["cvb.voltitle"]

    if (
        "cvb.title" in response_json
        and response_json["cvb.title"] is not None
        and len(response_json["cvb.title"]) != 0
    ):
        return response_json["cvb.title"]
    return None


def _extract_title_from_ceurws_org(volume_number: int) -> Optional[str]:
    url = "https://ceur-ws.org/Vol-" + str(volume_number)
    response = requests.get(url=url, timeout=60)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.content, "html.parser")
    tag = soup.find("span", class_="CEURVOLTITLE")
    if tag:
        return tag.get_text()
    return None


def extract_acronym(input_string: str) -> Optional[str]:
    pattern = r"\((.*?)\)"
    matches = re.search(pattern, input_string)

    if matches:
        return matches.group(1)

    return None
