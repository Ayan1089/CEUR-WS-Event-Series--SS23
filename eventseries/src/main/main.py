"""
Created on 2023-05-03

@author: Ayan1089, jkrude
"""
import importlib.resources as ires
import logging
import zipfile
from pathlib import Path

import spacy

from eventseries.src.main.completion.attribute_completion import complete_information
from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.matcher.acronym_matcher import AcronymMatch
from eventseries.src.main.matcher.dblp_matcher import DblpMatcher
from eventseries.src.main.matcher.ngram_matcher import NgramMatch
from eventseries.src.main.matcher.nlp_matcher import create_training_test_dataset, NlpMatcher
from eventseries.src.main.matcher.phrase_matcher import PhraseMatch
from eventseries.src.main.matcher.tfidf_matcher import TfIdfMatch
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


def spacy_package_exists():
    if not spacy.util.is_package("en_core_web_sm"):
        logging.error(
            "Could not find spacy package 'en_core_web_sm' please run"
            " 'python -m spacy download en_core_web_sm'"
        )
        return False
    return True


def extract_dblp_zip(zip_path: Path, extract_to: Path):
    logging.debug("Unzipping scraped dblp-html files")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


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

    zip_file_path = resource_dir / "dblp" / "conf.zip"
    with ires.as_file(zip_file_path) as zip_file, ires.as_file(dblp_context) as dblp_path:
        if zip_file_path.is_file():
            extract_dblp_zip(zip_file, dblp_path)


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
    matches = dblp_matcher.match_through_dblp()
    logging.info("Found %s many matches through dblp", len(matches))
    workshop_matches = len([m for m in matches if m.event.type == WikiDataEventType.WORKSHOP])
    conference_matches = len([m for m in matches if m.event.type == WikiDataEventType.CONFERENCE])
    logging.info(
        "Out of which %s were conferences and %s workshops", conference_matches, workshop_matches
    )
    matched_events = set(match.event.qid for match in matches)

    unmatched_events = [event for event in to_be_completed if event.qid not in matched_events]

    training_set = create_training_test_dataset(repository.get_matches())

    ngram_matcher = NgramMatch(matches_df=training_set)
    ngram_matches = ngram_matcher.match_events_to_series(
        event_list=unmatched_events, series_list=completed_series
    )
    logging.info("Found %s matched through n-grams.", len(ngram_matches))

    # Try phrase matching
    if spacy_package_exists():
        phrase_matcher = PhraseMatch(matches_df=training_set)
        phrase_matches = phrase_matcher.wikidata_match(unmatched_events, completed_series)
        logging.info("Found %s matched through phrase-matching.", len(phrase_matches))

        acronym_matches = AcronymMatch(training_set).wikidata_match(unmatched_events, completed_series)
        logging.info("Found %s matched through acronym-matching.", len(acronym_matches))

    tfidf_matches = TfIdfMatch(training_set).wikidata_match(unmatched_events, completed_series)
    logging.info("Found %s matched through tf-idf-matches.", len(tfidf_matches))

    # Use case scenario 1
    # series_completion = SeriesCompletion()
    # event_series = series_completion.get_event_series_from_ceur_ws_proceedings()

    nlp_matcher = NlpMatcher(training_set)
    nlp_matches = nlp_matcher.match(unmatched_events,completed_series)
    logging.info("Found %s matched through nlp-matches.", len(nlp_matches))
