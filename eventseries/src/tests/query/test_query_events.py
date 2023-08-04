"""
Created on 2023-05-01

@author: Ayan
"""
import unittest

from eventseries.src.main.query.queried_events import Events


class QueryEvents(unittest.TestCase):
    def test_query_events(self):
        events = Events()
        events_dict = events.read_as_dict()
        self.assertTrue(bool(events_dict), "Dictionary is empty")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
