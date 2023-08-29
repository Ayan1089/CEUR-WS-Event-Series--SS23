import tempfile
from importlib import resources as ires
from pathlib import Path
from unittest import TestCase

from eventseries.src.main.dblp.dblp_context import DblpContext
from eventseries.src.main.dblp.event_classes import DblpEvent
from eventseries.src.main.repository.dblp_respository import DblpRepository


class TestDblpRepository(TestCase):
    test_event = ires.files("eventseries.src.tests") / "resources" / "event.html"
    test_event_series = ires.files("eventseries.src.tests") / "resources" / "event_series.html"

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.conf_path = temp_path / "conf"
        self.conf_path.mkdir()

        self.dblp_path = temp_path / "dblp"
        self.dblp_path.mkdir()

        self.repo = DblpRepository(
            DblpContext(cache_file_path=self.conf_path), resource_dir=self.dblp_path
        )
        with TestDblpRepository.test_event.open("r") as test_file:
            self.test_event_content = test_file.read()
        with TestDblpRepository.test_event_series.open("r") as test_event_series_file:
            self.test_event_series_content = test_event_series_file.read()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_get_or_load_event(self):
        self.repo.ctx.cache_dblp_id("test", self.test_event_content)
        event = self.repo.get_or_load_event(dblp_id="test")
        self.assertIsInstance(event, DblpEvent)
        self.assertEqual(1, event.ordinal)
        self.assertEqual(2012, event.year)
        self.assertEqual("1. AT 2012", event.title)
        self.assertEqual("Dubrovnik, Croatia", event.location)

    def test_get_events_for_series_cached(self):
        test_dict = [
            {"dblp_id": "conf/test1", "content": "content", "name": "test1"},
            {"dblp_id": "conf/test1/event1", "content": "content", "name": "event1"},
            {"dblp_id": "conf/test2/event2", "content": "content", "name": "event2"},
            {"dblp_id": "conf/test1/event3", "content": "content", "name": "event3"},
            {"dblp_id": "journal/test1/event4", "content": "content", "name": "event4"},
        ]
        for test_event in test_dict:
            self.repo.ctx.cache_dblp_id(test_event["dblp_id"], test_event["content"])
            self.repo.cache_event(
                DblpEvent(test_event["name"], None, None, None, dblp_id=test_event["dblp_id"])
            )

        events = self.repo.get_events_for_series("conf/test1")

        for event in events:
            self.assertTrue(event.dblp_id in ["conf/test1/event1", "conf/test1/event3"])

    def test_load_cached_event(self):
        self.repo.ctx.cache_dblp_id("test", self.test_event_content)
        dblp_event = self.repo.get_or_load_event("test")
        self.assertIsInstance(dblp_event, DblpEvent)
        self.repo.store_cached(overwrite=True)
        self.assertTrue((self.dblp_path / "events.pickle").is_file())
        fresh_repo = DblpRepository(self.repo.ctx, resource_dir=self.dblp_path, load_on_init=True)
        fresh_repo.is_cached(DblpRepository.EVENTS)
        fresh_repo.is_cached(DblpRepository.EVENT_SERIES)
        self.assertEqual(0, len(fresh_repo.event_series))
        self.assertEqual(1, len(fresh_repo.events))
        self.assertEqual(dblp_event, fresh_repo.events.get("test"))

    def test_update_event_series_from_context_no_arg(self):
        self.repo.ctx.cache_dblp_id("conf/aaai", self.test_event_series_content)
        self.repo.update_event_series_from_context()
        self.assertTrue("conf/aaai" in self.repo.event_series)
        self.assertEqual(
            "aaai", self.repo.get_or_load_event_series("conf/aaai").abbreviation.lower()
        )
        # test that update_event_series_from_context does not remove information
        event_series = self.repo.get_or_load_event_series("conf/aaai")
        self.repo.ctx.dblp_cache.clear()
        self.repo.update_event_series_from_context()
        self.assertEqual(event_series, self.repo.get_or_load_event_series("conf/aaai"))
