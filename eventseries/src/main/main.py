"""
Created on 2023-05-03

@author: Ayan1089, jkrude
"""
from typing import List

from eventseries.src.main.completion.attribute_completion import (
    complete_ordinals,
    complete_ceurws_title,
)
from eventseries.src.main.completion.series_completion import SeriesCompletion
from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.matcher.full_matcher import full_matches
from eventseries.src.main.repository.completion_cache import CompletionCache
from eventseries.src.main.repository.dblp_respository import DblpRepository
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import WikiDataEvent, WikiDataProceeding
from eventseries.src.main.repository.wikidata_query_manager import WikiDataQueryManager


def complete_information(repo: Repository):
    events: List[WikiDataEvent] = list(repo.events_by_qid.values())
    proceedings: List[WikiDataProceeding] = list(repo.proceeding_by_event_qid.values())

    # try to extract the ordinal for events
    for ordinal_completion in complete_ordinals(events):
        repo.completion_cache.add_completion(ordinal_completion)
    # add ceurws_title to all proceedings
    for ceurws_title_completion in complete_ceurws_title(proceedings):
        repo.completion_cache.add_completion(ceurws_title_completion)


if __name__ == "__main__":
    repository = Repository(
        query_manager=WikiDataQueryManager(),
        dblp_repo=DblpRepository(dblp_context=DblpContext()),
        completion_cache=CompletionCache(),
    )

    complete_information(repository)

    completed_events = [
        repository.get_event_by_qid(qid=qid, patched=True)
        for qid in repository.events_by_qid.keys()
    ]
    completed_series = [
        repository.get_event_series_by_qid(qid=qid, patched=True)
        for qid in repository.event_series_by_qid.keys()
    ]

    # Extract full matches
    for full_match in full_matches(completed_events, completed_series):
        repository.completion_cache.add_match(full_match)

    # Use case scenario 1
    series_completion = SeriesCompletion()
    event_series = series_completion.get_event_series_from_ceur_ws_proceedings()

    # nlp matches FIXME
    #nlp_matcher = NlpMatcher(event_extractor, matcher)
    #nlp_matcher.match(utility.read_event_titles(), utility.read_event_series_titles())
