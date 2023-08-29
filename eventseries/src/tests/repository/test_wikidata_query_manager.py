import tempfile
import unittest
from pathlib import Path
from typing import List, Dict
from unittest import TestCase

from eventseries.src.main.repository.wikidata_query_manager import WikiDataQueryManager


class TestWikiDataQueryManager(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)

        self.manager = WikiDataQueryManager(resource_dir=temp_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @unittest.SkipTest
    def test_validate_event_queries(self):
        """Not tested automatically due possible long duration"""
        manager = self.manager
        events = manager.wikidata_all_ceurws_events()
        conferences = manager.wikidata_conferences()
        workshops = manager.wikidata_workshops()
        self.assertIsInstance(events, List)
        first_event = events[0]
        self.assertIsInstance(first_event, Dict)
        self.assertTrue("event" in first_event)
        self.assertTrue("eventLabel" in first_event)
        for workshop in workshops:
            self.assertIn(workshop, events)

        for conference in conferences:
            self.assertIn(conference, events)

        # Assert that workshops and conferences are disjoint
        # NOTE they aren't there are some ambiguous conferences classified as workshop
        self.assertEqual(
            0,
            len(
                set(conf["event"] for conf in conferences).intersection(
                    set(work["event"] for work in workshops)
                )
            ),
        )

    def test_validate_event_series_queries(self):
        manager = self.manager
        event_series = manager.wikidata_all_ceurws_event_series()
        conferences = manager.wikidata_conference_series()
        workshops = manager.wikidata_workshop_series()
        self.assertIsInstance(event_series, List)
        first_event = event_series[0]
        self.assertIsInstance(first_event, Dict)
        self.assertTrue(
            all("series" in series and "seriesLabel" in series for series in event_series)
        )
        for workshop in workshops:
            self.assertIn(workshop, event_series)

        for conference in conferences:
            self.assertIn(conference, event_series)

        # Assert that workshop-series and conference-series are disjoint
        self.assertEqual(
            0,
            len(
                set(conf["series"] for conf in conferences).intersection(
                    set(work["series"] for work in workshops)
                )
            ),
        )

    @unittest.SkipTest
    def test_validate_proceedings_query(self):
        proceedings = self.manager.wikidata_all_proceedings()
        self.assertIsInstance(proceedings, List)
        self.assertIsInstance(proceedings[0], Dict)
        self.assertTrue(all("event" in series and "proceeding" in series for series in proceedings))
        events = self.manager.wikidata_all_ceurws_events()
        event_ids = set(event["event"] for event in events)
        # test that all events of proceedings are also included in the events query
        for p_event in proceedings:
            self.assertIn(p_event["event"], event_ids)
