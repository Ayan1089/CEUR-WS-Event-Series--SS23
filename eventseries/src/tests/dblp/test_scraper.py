from unittest import TestCase

import bs4

from eventseries.src.main.dblp.scraper import DblpScraper
from eventseries.src.main.dblp.venue_information import VenueInformation
from eventseries.src.main.util.validation import dataframe_contains_columns
from eventseries.src.tests.dblp.test_dblp_context import ManagedDblpContext


class TestDblpScraper(TestCase):
    def test_extract_venue_information_of_cached_emtpy_context(self):
        # Test method with a completely empty context.
        with ManagedDblpContext() as empty_context:
            scraper = DblpScraper(empty_context)
            extracted_df = scraper.extract_venue_information_of_cached()
            self.assertTrue(dataframe_contains_columns(extracted_df, ["dblp_id", "tag", "venue"]))
            self.assertEqual(0, len(extracted_df))

    def test_extract_venue_information_of_cached_invalid_content(self):
        # Test method with a context that has invalid content.
        with ManagedDblpContext() as bad_context:
            bad_context.cache_dblp_id("conf/test", "invalid html")
            scraper = DblpScraper(bad_context)
            extracted_df = scraper.extract_venue_information_of_cached()
            self.assertTrue(dataframe_contains_columns(extracted_df, ["dblp_id", "tag", "venue"]))
            self.assertEqual(0, len(extracted_df))

    def test_extract_venue_information_of_cached_valid_content(self):
        # Test method with one single valid entry.
        with ManagedDblpContext() as bad_context:
            html_content = """
            <div id="info-section" class="section">
                <div class="stream-info hideable tts-content">
                    <header class="hide-head h2 sub">
                        <h2>Venue Information</h2>
                    </header>
                    <div class="hide-body">
                        <ul><li><em>has part (2010, 2013, 2014, 2020):</em> <a href="https://dblp.org/db/conf/starai/index.html">International Workshop on Statistical Relational AI (StarAI)</a></li></ul>
                    </div>
                </div>
            </div>"""
            bad_context.cache_dblp_id("conf/test", html_content)
            scraper = DblpScraper(bad_context)
            extracted_df = scraper.extract_venue_information_of_cached()
            self.assertTrue(dataframe_contains_columns(extracted_df, ["dblp_id", "tag", "venue"]))
            self.assertEqual(1, len(extracted_df.dblp_id))
            self.assertEqual("conf/test", extracted_df.dblp_id.iloc[0])
            self.assertIsInstance(extracted_df.tag.iloc[0], bs4.Tag)
            self.assertEqual(extracted_df.tag.iloc[0].get("id"), "info-section")
            self.assertTrue(extracted_df.tag.iloc[0].find("em").string.startswith("has part"))
            self.assertIsInstance(extracted_df.venue.iloc[0], VenueInformation)
            self.assertIsInstance(extracted_df.venue.iloc[0], VenueInformation)
            venue_info_parsed: VenueInformation = extracted_df.venue.iloc[0]
            # correct parsing of has_part not the task of this method and tested in parsing
            self.assertEqual(0, len(venue_info_parsed.access))
            self.assertEqual(0, len(venue_info_parsed.is_part_of))
            self.assertEqual(0, len(venue_info_parsed.not_to_be_confused_with))
            self.assertEqual(0, len(venue_info_parsed.predecessor))
            self.assertEqual(0, len(venue_info_parsed.related))
            self.assertEqual(0, len(venue_info_parsed.status))
            self.assertEqual(0, len(venue_info_parsed.successor))
            self.assertEqual(1, len(venue_info_parsed.has_part))
