"""
Created on 2023-05-01

@author: Ayan
"""
import unittest

from eventseries.src.main.matcher.wikidata_matcher import Matcher


class TestMatcher(unittest.TestCase):
    """
    test matching
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testMatcher(self):
        """
        test matcher
        """
        matcher = Matcher()
        self.assertTrue(matcher is not None)
        pass


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
