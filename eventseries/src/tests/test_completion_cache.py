import tempfile
from pathlib import Path
from typing import Dict, List
from unittest import TestCase

from eventseries.src.main.repository.completion_cache import CompletionCache
from eventseries.src.main.repository.completions import WithAcronym, WithOrdinal, Match


class TestCompletionCache(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path: Path = Path(self.temp_dir.name)
        self.repo = CompletionCache(
            resource_dir=self.temp_path, load_on_init=True, store_on_delete=True
        )
        self.with_acronym = WithAcronym(qid="Q123", acronym="test", found_by="TestAlgo")
        self.match = Match(
            series={"title": "TestSeries", "series": "http://www.wikidata.org/entity/Q0"},
            event={"title": "TestEvent", "event": "http://www.wikidata.org/entity/Q123"},
            found_by="TestAlgo",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_add_completion_for_qid(self):
        self.assertEqual(0, len(self.repo.cache[CompletionCache.ITEM_COMPLETION]))
        self.repo.add_completion_for_qid(qid="Q123", completion=self.with_acronym)
        self.assertEqual(1, len(self.repo.cache[CompletionCache.ITEM_COMPLETION]))
        self.assertEqual(
            self.with_acronym, self.repo.cache[CompletionCache.ITEM_COMPLETION]["Q123"][0]
        )

    def test_get_completions_for_qid(self):
        self.repo.add_completion_for_qid("Q123", self.with_acronym)
        self.repo.add_completion_for_qid(
            "Q100", WithOrdinal(qid="Q100", ordinal=1, found_by="TestAlgo")
        )
        completions = self.repo.get_completions_for_qid("Q123")
        self.assertEqual(1, len(completions))
        self.assertEqual(self.with_acronym, completions[0])

    def test_get_all_matches(self):
        self.repo.add_completion_for_qid("Q123", self.with_acronym)
        self.repo.add_match(self.match)
        matches = self.repo.get_all_matches()
        self.assertIsInstance(matches, List)
        self.assertEqual(1, len(matches))
        self.assertIsInstance(matches[0], Match)
        self.assertEqual(self.match, matches[0])

    def test_get_matches_by_source(self):
        self.assertIsInstance(self.repo.get_matches_by_source(), Dict)
        self.repo.add_match(self.match)
        match_dict = self.repo.get_matches_by_source()
        self.assertIsInstance(match_dict, Dict)
        self.assertTrue(self.match.found_by in match_dict)
        self.assertEqual(self.match, match_dict[self.match.found_by])

    def test_add_match(self):
        self.assertEqual(0, len(self.repo.cache[CompletionCache.MATCHES]))
        self.repo.add_match(self.match)
        self.assertEqual(1, len(self.repo.cache[CompletionCache.MATCHES]))
        self.assertEqual(self.match, self.repo.cache[CompletionCache.MATCHES][0])

    def test_store_load_cache(self):
        # assert directory is empty at start
        self.assertEqual(0, len(list(self.temp_path.iterdir())))

        self.repo.add_match(self.match)
        self.repo.add_completion_for_qid("Q123", self.with_acronym)
        self.repo.cache_content("additional", "content")

        self.repo.store_cached(overwrite=True)
        # assert that something was written
        fresh_repo = CompletionCache(resource_dir=self.temp_path, load_on_init=True)
        completions = fresh_repo.get_completions_for_qid(qid="Q123")
        self.assertEqual(1, len(completions))
        self.assertEqual(self.with_acronym, completions[0])
        matches = fresh_repo.get_all_matches()
        self.assertEqual(1, len(matches))
        self.assertEqual(self.match, matches[0])
        self.assertEqual("content", self.repo.get_cached("additional"))
