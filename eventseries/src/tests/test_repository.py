import tempfile
from pathlib import Path
from unittest import TestCase

from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.repository.completion_cache import CompletionCache
from eventseries.src.main.repository.completions import WithAcronym, Match
from eventseries.src.main.repository.dblp_respository import DblpRepository
from eventseries.src.main.repository.repository import Repository
from eventseries.src.main.repository.wikidata_dataclasses import (
    QID,
    WikiDataEventType,
    WikiDataEvent,
    WikiDataProceeding,
    WikiDataEventSeries,
)
from eventseries.src.main.repository.wikidata_query_manager import WikiDataQueryManager


class TestRepository(TestCase):
    temp_path = None
    temp_dir = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_path = Path(cls.temp_dir.name)
        completions = cls.temp_path / "completions"
        dblp = cls.temp_path / "dblp"
        dblp_raw = dblp / "raw"
        dblp_parsed = dblp / "parsed"
        query = cls.temp_path / "query"
        completions.mkdir()
        dblp_raw.mkdir(parents=True)
        dblp_parsed.mkdir(parents=True)
        query.mkdir()
        # create only once in order to reduce network connections
        cls.query_manager = WikiDataQueryManager(resource_dir=query)

    def setUp(self) -> None:
        self.repo = Repository(
            query_manager=self.query_manager,
            dblp_repo=DblpRepository(
                dblp_context=DblpContext(cache_file_path=self.temp_path / "dblp" / "raw"),
                resource_dir=self.temp_path / "dblp" / "parsed",
            ),
            completion_cache=CompletionCache(resource_dir=self.temp_path / "completions"),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def test_get_event_by_qid(self):
        # 10th International Workshop on Science Gateways
        qid = QID("Q106245681")
        event = self.repo.get_event_by_qid(qid)
        self.assertIsInstance(event, WikiDataEvent)
        self.assertEqual(qid, event.qid)

    def test_get_event_series_by_qid(self):
        qid = QID("Q113625339")
        series = self.repo.get_event_series_by_qid(qid, patched=False)
        self.assertIsInstance(series, WikiDataEventSeries)
        self.assertEqual(qid, series.qid)
        self.assertEqual(
            "Workshop on Managing the Evolution and Preservation of the Data Web", series.label
        )
        self.assertEqual("MEPDaW", series.acronym)
        self.assertEqual([QID("Q47459256")], series.instance_of)

        missing_acronym_qid = QID("Q105698882")
        self.repo.completion_cache.add_completion(
            WithAcronym(missing_acronym_qid, acronym="IWSG", found_by=None)
        )
        missing_acronym_series = self.repo.get_event_series_by_qid(
            missing_acronym_qid, patched=False
        )
        self.assertIsNone(missing_acronym_series.acronym)
        self.assertEqual(
            "IWSG", self.repo.get_event_series_by_qid(missing_acronym_qid, patched=True).acronym
        )

    def test_get_proceeding_by_qid(self):
        proceeding = self.repo.get_proceeding_by_qid(QID("Q113544823"), patched=False)
        self.assertIsInstance(proceeding, WikiDataProceeding)
        self.assertEqual(QID("Q113649424"), proceeding.event)

    def test_get_dblp_event_by_id(self):
        dblp_id = "conf/iwsg/iwsg2013"
        dblp_event = self.repo.get_dblp_event_by_id(dblp_id=dblp_id)
        self.assertEqual(dblp_id, dblp_event.dblp_id)
        self.assertEqual(5, dblp_event.ordinal)
        self.assertEqual(2013, dblp_event.year)

    def test_get_dblp_event_series_by_id(self):
        dblp_id = "conf/iwsg"
        dblp_event_series = self.repo.get_dblp_event_series_by_id(dblp_id)
        self.assertEqual(dblp_id, dblp_event_series.dblp_id)

    def test_get_matches(self):
        self.assertEqual([], self.repo.get_matches())
        # 10th International Workshop on Science Gateways
        event = self.repo.get_event_by_qid(QID("Q106245681"))

        series = self.repo.get_event_series_by_qid(QID("Q105698882"))
        self.repo.completion_cache.add_match(Match(event=event, series=series, found_by="Wikidata"))

    def test_add_type_to_events_and_series(self):
        # As there are some events which are classified both conferences and workshops only
        # series are tested here
        for workshop_series in self.repo.query_manager.wikidata_workshop_series():
            series = self.repo.get_event_series_by_qid(workshop_series.qid)
            self.assertEqual(WikiDataEventType.WORKSHOP, series.type)

        for conference_series in self.repo.query_manager.wikidata_conference_series():
            series = self.repo.get_event_series_by_qid(conference_series.qid)
            self.assertEqual(WikiDataEventType.CONFERENCE, series.type)
