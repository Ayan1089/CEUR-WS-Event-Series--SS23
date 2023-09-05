import tempfile
from pathlib import Path
from typing import List
from unittest import TestCase

from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataProceeding,
    WikiDataEvent,
    WikiDataEventSeries,
    WikiDataEventType,
)
from eventseries.src.main.repository.wikidata_query_manager import WikiDataQueryManager


class TestWikiDataQueryManager(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)

        self.manager = WikiDataQueryManager(resource_dir=temp_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_validate_event_queries(self):
        """Not tested automatically due possible long duration"""
        manager = self.manager
        events = manager.wikidata_all_ceurws_events()
        event_ids = set(series.qid for series in events)
        conferences = manager.wikidata_conferences()
        conf_ids = set(conference.qid for conference in conferences)
        workshops = manager.wikidata_workshops()
        workshop_ids = set(workshop.qid for workshop in workshops)
        self.assertIsInstance(events, List)
        self.assertTrue(all(isinstance(item, WikiDataEvent) for item in events))

        for workshop_id in workshop_ids:
            self.assertIn(workshop_id, event_ids)

        for conference_id in conf_ids:
            self.assertIn(conference_id, event_ids)

        # Assert that workshops and conferences are disjoint
        # NOTE they aren't there are some ambiguous conferences classified as workshop
        self.assertEqual(13, len(workshop_ids.intersection(conf_ids)))

        # Assert that conferences are marked as conferences
        self.assertTrue(all(conf.type == WikiDataEventType.CONFERENCE for conf in conferences))
        # Assert that workshops are marked as workshops
        self.assertTrue(all(workshop.type == WikiDataEventType.WORKSHOP for workshop in workshops))

        manager.store_cached(overwrite=True)
        fresh_manager = WikiDataQueryManager(resource_dir=manager.resource_dir, load_on_init=True)
        self.assertEqual(events, fresh_manager.get_cached(WikiDataQueryManager.EVENTS))
        self.assertEqual(conferences, fresh_manager.get_cached(WikiDataQueryManager.CONFERENCES))
        self.assertEqual(workshops, fresh_manager.get_cached(WikiDataQueryManager.WORKSHOPS))

    def test_validate_event_series_queries(self):
        manager = self.manager
        event_series = manager.wikidata_all_ceurws_event_series()
        event_series_ids = set(series.qid for series in event_series)
        conference_series = manager.wikidata_conference_series()
        conf_ids = set(conference.qid for conference in conference_series)
        workshop_series = manager.wikidata_workshop_series()
        workshop_ids = set(workshop.qid for workshop in workshop_series)
        self.assertIsInstance(event_series, List)
        self.assertTrue(all(isinstance(item, WikiDataEventSeries) for item in event_series))

        for workshop_id in workshop_ids:
            self.assertIn(workshop_id, event_series_ids)

        for conference_id in conf_ids:
            self.assertIn(conference_id, event_series_ids)

        # Assert that workshop-series and conference-series are disjoint
        self.assertEqual(0, len(workshop_ids.intersection(conf_ids)))

        # Assert that conferences are marked as conferences
        self.assertTrue(
            all(conf.type == WikiDataEventType.CONFERENCE for conf in conference_series)
        )
        # Assert that workshops are marked as workshops
        self.assertTrue(
            all(workshop.type == WikiDataEventType.WORKSHOP for workshop in workshop_series)
        )

    def test_validate_proceedings_query(self):
        proceedings = self.manager.wikidata_all_proceedings()
        self.assertIsInstance(proceedings, List)
        self.assertTrue(all(isinstance(event, WikiDataProceeding) for event in proceedings))
        events: List[WikiDataEvent] = self.manager.wikidata_all_ceurws_events()
        event_ids = set(event.qid for event in events)
        # test that all events of proceedings are also included in the events query
        for p_event in proceedings:
            self.assertIn(p_event.event, event_ids)
