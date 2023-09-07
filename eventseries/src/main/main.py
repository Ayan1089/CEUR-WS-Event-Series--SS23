"""
Created on 2023-05-03

@author: Ayan1089, jkrude
"""
import importlib.resources as ires
import logging
import time
import zipfile
from pathlib import Path

from eventseries.src.main.completion.attribute_completion import complete_information
from eventseries.src.main.completion.series_completion import SeriesCompletion
from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.matcher.dblp_matcher import DblpMatcher
from eventseries.src.main.matcher.full_matcher import full_matches
from eventseries.src.main.matcher.nlp_matcher import create_training_test_dataset, NlpMatcher
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


def use_zip_if_no_dblp_context(zip_source_path, target_path):
    with ires.as_file(target_path / "conf") as conf_file:
        if conf_file.exists():
            return
        logging.info("Extracting zip archive of dblp content.")
    with ires.as_file(zip_source_path) as zip_file, ires.as_file(target_path) as dblp_file:
        if zip_file.is_file():
            extract_dblp_zip(zip_file, dblp_file)


def extract_dblp_zip(zip_path: Path, extract_to: Path):
    logging.debug("Unzipping scraped dblp-html files")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Create the directories for persistence.
    resource_dir = ires.files("eventseries.src.main") / "resources"
    query_dir = resource_dir / "query_results"
    dblp_path = resource_dir / "dblp"
    dblp_repo = dblp_path / "parsed"
    dblp_context = dblp_path / "conf"
    completion_dir = resource_dir / "completions"
    for dir_traversable in (query_dir, dblp_repo, dblp_context, completion_dir):
        with ires.as_file(dir_traversable) as file:
            file.mkdir(parents=True, exist_ok=True)

    use_zip_if_no_dblp_context(resource_dir / "dblp" / "conf.zip", dblp_path)

    repository = Repository(
        query_manager=WikiDataQueryManager(),
        dblp_repo=DblpRepository(dblp_context=DblpContext()),
        completion_cache=CompletionCache(),
    )

    fix_known_errors(repository)

    # Requesting over 3000 files from dblp can take a lot of time!
    # scrape_wikidata_with_dblp_id(repository)

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
    dblp_matches = dblp_matcher.match_through_dblp()
    logging.info("Found %s many matches through dblp", len(dblp_matches))
    workshop_matches = len([m for m in dblp_matches if m.event.type == WikiDataEventType.WORKSHOP])
    conference_matches = len(
        [m for m in dblp_matches if m.event.type == WikiDataEventType.CONFERENCE]
    )
    logging.info(
        "Out of which %s were conferences and %s workshops", conference_matches, workshop_matches
    )
    matched_events = set(match.event.qid for match in dblp_matches)

    unmatched_events = [event for event in to_be_completed if event.qid not in matched_events]

    full_matches_found = full_matches(unmatched_events, completed_series)
    logging.info("Found %s matches through full-matches.", len(full_matches_found))

    for match in full_matches_found:
        matched_events.add(match.event.qid)
    # All events that were neither matched by dblp nor through full matches
    unmatched_events = [event for event in unmatched_events if event.qid not in matched_events]
    logging.info("There are %s remaining unmatched events", len(unmatched_events))

    training_set = create_training_test_dataset(dblp_matches + full_matches_found)
    logging.info("Created training set of %s entries.", len(training_set))

    nlp_matcher = NlpMatcher(training_set)
    nlp_matches = nlp_matcher.match(unmatched_events, completed_series)
    logging.info("Found %s matched through nlp-matches.", len(nlp_matches))

    # Use case scenario 1
    series_completion = SeriesCompletion(repository)
    event_series = series_completion.get_event_series_from_ceur_ws_proceedings()

    del repository
    time.sleep(2)  # give repository time to save before python shuts down
