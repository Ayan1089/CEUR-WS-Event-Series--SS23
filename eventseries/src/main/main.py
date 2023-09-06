"""
Created on 2023-05-03

@author: Ayan1089, jkrude
"""
import importlib.resources as ires
import logging

from eventseries.src.main.completion.attribute_completion import complete_information
from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.matcher.dblp_matcher import DblpMatcher
from eventseries.src.main.matcher.full_matcher import full_matches
from eventseries.src.main.repository.completion_cache import CompletionCache
from eventseries.src.main.repository.dblp_respository import DblpRepository
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import QID, WikiDataEventType
from eventseries.src.main.repository.wikidata_query_manager import WikiDataQueryManager


def fix_known_errors(repo: Repository):
    # This workshop has an invalid dblp id the original has one d to much in "conf/gvd/gvdb2021".
    gvd_workshop_32 = repo.events_by_qid.get(QID("Q113580007"))
    if gvd_workshop_32 is None:
        return
    gvd_workshop_32.dblp_id = "conf/gvd/gvd2021"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Create the directories for persistence.
    resource_dir = ires.files("eventseries.src.main") / "resources"
    query_dir = resource_dir / "query_results"
    dblp_repo = resource_dir / "dblp" / "parsed"
    dblp_context = resource_dir / "dblp" / "conf"
    completion_dir = resource_dir / "completions"
    for dir_traversable in (query_dir, dblp_repo, dblp_context, completion_dir):
        with ires.as_file(dir_traversable) as file:
            file.mkdir(parents=True, exist_ok=True)

    repository = Repository(
        query_manager=WikiDataQueryManager(),
        dblp_repo=DblpRepository(dblp_context=DblpContext()),
        completion_cache=CompletionCache(),
    )

    fix_known_errors(repository)

    # scrape_wikidata_with_dblp_id(repository)

    # scrape_dblp(repository)
    complete_information(repository)

    completed_events = [
        repository.get_event_by_qid(qid=qid, patched=True)
        for qid in repository.events_by_qid.keys()
    ]
    completed_series = [
        repository.get_event_series_by_qid(qid=qid, patched=True)
        for qid in repository.event_series_by_qid.keys()
    ]

    # Match through dblp
    to_be_completed = repository.events_without_series(ignore_match_completions=True)

    dblp_matcher = DblpMatcher(repository=repository, to_be_matched=to_be_completed)
    matches = dblp_matcher.match_through_dblp()
    logging.info("Found %s many matches through dblp", len(matches))
    workshop_matches = len([m for m in matches if m.event.type == WikiDataEventType.WORKSHOP])
    conference_matches = len([m for m in matches if m.event.type == WikiDataEventType.CONFERENCE])
    logging.info(
        "Out of which %s were conferences and %s workshops", conference_matches, workshop_matches
    )

    # Extract full matches
    utility = Utility()
    event_extractor = EventExtractor()
    matcher = Matcher()
    full_matcher = FullMatch(utility, event_extractor, matcher)
    full_matcher.match(records)

    # Use case scenario 1
    series_completion = SeriesCompletion()
    event_series = series_completion.get_event_series_from_ceur_ws_proceedings()

    # nlp matches FIXME
    # nlp_matcher = NlpMatcher(event_extractor, matcher)
    # nlp_matcher.match(utility.read_event_titles(), utility.read_event_series_titles())
