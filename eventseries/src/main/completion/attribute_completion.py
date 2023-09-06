from dataclasses import asdict
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from plp.ordinal import Ordinal

from eventseries.src.main.repository.completions import WithOrdinal, WithCeurWsTitle
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import WikiDataEvent, WikiDataProceeding


def complete_information(repo: Repository):
    events: List[WikiDataEvent] = list(repo.events_by_qid.values())
    proceedings: List[WikiDataProceeding] = list(repo.proceeding_by_event_qid.values())

    # try to extract the ordinal for events
    without_ordinal_completion = [
        event
        for event in events
        if not any(
            isinstance(comp, WithOrdinal)
            for comp in repo.completion_cache.get_completions_for_qid(event.qid)
        )
    ]
    for ordinal_completion in complete_ordinals(without_ordinal_completion):
        repo.completion_cache.add_completion(ordinal_completion)

    # add ceurws_title to all proceedings
    without_ceurws_title_completion = [
        proceeding
        for proceeding in proceedings
        if not any(
            isinstance(comp, WithCeurWsTitle)
            for comp in repo.completion_cache.get_completions_for_qid(proceeding.qid)
        )
    ]
    for ceurws_title_completion in complete_ceurws_title(without_ceurws_title_completion):
        repo.completion_cache.add_completion(ceurws_title_completion)


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
