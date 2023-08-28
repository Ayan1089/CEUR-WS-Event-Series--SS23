import tempfile
import unittest
from pathlib import Path
from typing import Optional
from unittest.mock import patch, Mock

from requests import Response

from eventseries.src.main.dblp.dblp_context import DblpContext


class ManagedDblpContext:
    def __init__(self):
        self.tmp_dir_parent: Optional[tempfile.TemporaryDirectory] = None

    def __enter__(self):
        self.tmp_dir_parent = tempfile.TemporaryDirectory()
        parent_path = Path(self.tmp_dir_parent.name)
        test_cache_path = parent_path / "dblp"
        test_cache_path.mkdir()
        dblp_context = DblpContext(cache_file_path=test_cache_path, load_cache=False)
        return dblp_context

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tmp_dir_parent.cleanup()


class TestDblpContext(unittest.TestCase):
    def setUp(self):
        self.tmp_dir_parent = tempfile.TemporaryDirectory()
        parent_path = Path(self.tmp_dir_parent.name)
        self.test_cache_path = parent_path / "dblp"
        self.test_cache_path.mkdir()

        self.test_series_id = "conf/testseries"
        self.dblp_context = DblpContext(cache_file_path=self.test_cache_path,
                                        load_cache=False)

    def tearDown(self):
        self.tmp_dir_parent.cleanup()

    def test_valid_object_creation(self):
        dblp_base = "https://dblp.org/db/"
        cache_file_path = self.test_cache_path
        dblp_context = DblpContext(dblp_base=dblp_base, cache_file_path=cache_file_path)
        self.assertIsInstance(dblp_context, DblpContext)

    def test_invalid_dblp_base(self):
        invalid_dblp_base = "invalid_url"
        with self.assertRaises(ValueError):
            DblpContext(dblp_base=invalid_dblp_base)

    def test_nonexistent_cache_path(self):
        invalid_cache_path = Path("nonexistent_path")
        with self.assertRaises(ValueError):
            DblpContext(dblp_base="https://dblp.org/db/", cache_file_path=invalid_cache_path)

    def test_non_directory_cache_path(self):
        with tempfile.NamedTemporaryFile() as tmp_file:
            file_path = Path(tmp_file.name)
            self.assertTrue(file_path.exists())
            with self.assertRaises(ValueError):
                DblpContext(dblp_base="https://dblp.org/db/", cache_file_path=file_path)

    def test_validate_and_clean_dblp_id(self):
        valid_id = "conf/validconf"
        cleaned_id = DblpContext._validate_and_clean_dblp_id(valid_id)
        self.assertEqual(cleaned_id, valid_id)

        invalid_id = "https://example.com"
        with self.assertRaises(ValueError):
            DblpContext._validate_and_clean_dblp_id(invalid_id)

    def test_cache_dblp_id(self):
        dblp_id = "conf/test"
        content = "cached content"
        self.dblp_context.cache_dblp_id(dblp_id, content)
        self.assertIn(dblp_id, self.dblp_context.dblp_cache)
        self.assertEqual(self.dblp_context.dblp_cache[dblp_id], content)
        self.assertTrue(self.dblp_context.is_cached(dblp_id))

    def test_is_cached(self):
        self.dblp_context.cache_dblp_id("conf/test1", "content")
        self.dblp_context.cache_dblp_id("conf/test2/event1", "content")

        self.assertTrue(self.dblp_context.is_cached("conf/test1"))
        self.assertFalse(self.dblp_context.is_cached("conf/test2"))
        self.assertFalse(self.dblp_context.is_cached("conf/nonexistent"))

    def test_get_cached(self):
        dblp_id = "conf/test"
        content = "cached content"
        self.dblp_context.cache_dblp_id(dblp_id, content)
        cached_content = self.dblp_context.get_cached(dblp_id)
        self.assertEqual(cached_content, content)

    def test_get_cached_nonexistent(self):
        dblp_id = "conf/nonexistent"
        with self.assertRaises(ValueError):
            self.dblp_context.get_cached(dblp_id)

    def test_store_cache(self):
        self.dblp_context.cache_dblp_id("conf/test1", "content1")
        self.dblp_context.cache_dblp_id("conf/test2", "content2")
        self.dblp_context.cache_dblp_id("conf/test1/event1", "content3")
        self.dblp_context.cache_dblp_id("conf/test3/event2", "content4")
        self.dblp_context.store_cache()

        for key, content in self.dblp_context.dblp_cache.items():
            file_path = self.dblp_context.dblp_base_path / Path(key).with_suffix(".html")
            self.assertTrue(file_path.exists())
            with file_path.open() as file:
                stored_content = file.read()
                self.assertEqual(stored_content, content)

    def test_get_cached_series_keys(self):
        self.dblp_context.cache_dblp_id("conf/test1", "content")
        self.dblp_context.cache_dblp_id("conf/test2", "content")
        self.dblp_context.cache_dblp_id("conf/test/event", "content")
        self.dblp_context.cache_dblp_id("conf/test1/event2", "content")

        series_keys = self.dblp_context.get_cached_series_keys()
        self.assertEqual(series_keys, ["conf/test1", "conf/test2"])

    def test_get_events_for_series(self):
        self.dblp_context.cache_dblp_id("conf/test1", "content")
        self.dblp_context.cache_dblp_id("conf/test1/event1", "content")
        self.dblp_context.cache_dblp_id("conf/test2/event2", "content")
        self.dblp_context.cache_dblp_id("conf/test1/event3", "content")
        self.dblp_context.cache_dblp_id("journal/test1/event4", "content")
        events = self.dblp_context.get_events_for_series("conf/test1")
        self.assertEqual(events, ["conf/test1/event1", "conf/test1/event3"])

    def test_get_series_with_events(self):
        self.dblp_context.cache_dblp_id("conf/test1", "content")
        self.dblp_context.cache_dblp_id("conf/test1/event1", "content")
        self.dblp_context.cache_dblp_id("conf/test2", "content")
        self.dblp_context.cache_dblp_id("conf/test1/event2", "content")
        self.dblp_context.cache_dblp_id("journal/test/event3", "content")
        series_with_events = self.dblp_context.get_series_with_events(["conf/test1"])
        self.assertEqual(series_with_events,
                         {"conf/test1": ["conf/test1/event1", "conf/test1/event2"]})
        self.assertEqual(self.dblp_context.get_series_with_events(),
                         {"conf/test1": ["conf/test1/event1", "conf/test1/event2"],
                          "conf/test2": []})

    @patch("requests.get")
    def test_request_dblp(self, mock_get):
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.text = "Mocked response text"
        mock_get.return_value = mock_response

        url = "https://dblp.org/db/conf/test"
        response_text = DblpContext.request_dblp(url)
        self.assertEqual(response_text, mock_response.text)

    @patch("requests.get")
    def test_failing_request_dblp(self, mock_get):
        mock_response = Mock(spec=Response)
        mock_response.status_code = 400
        mock_get.return_value = mock_response

        url = "https://dblp.org/db/conf/test"
        with self.assertRaises(ValueError):
            DblpContext.request_dblp(url, retry=False)

    @patch("time.sleep")
    @patch("requests.get")
    def test_request_dblp_retry(self, mock_get, mock_sleep):
        # Mock for the first call (status code 429)
        mock_response_429 = Mock(spec=Response)
        mock_response_429.status_code = 429
        mock_response_429.headers = {"Retry-After": "5"}

        # Mock for the second call (status code 200)
        mock_response_200 = Mock(spec=Response)
        mock_response_200.status_code = 200
        mock_response_200.text = "Mocked response text"

        # Configure side effects for the mock_get function
        mock_get.side_effect = [mock_response_429, mock_response_200]

        url = "https://dblp.org/db/conf/test"
        response_text = self.dblp_context.request_dblp(url, retry=True)

        self.assertEqual(response_text, mock_response_200.text)
        mock_get.assert_called_with(url, timeout=120)
        mock_sleep.assert_called_with(5)

    @patch("eventseries.src.main.dblp.dblp_context.DblpContext.request_dblp")
    def test_request_or_load_dblp_cached(self, mock_request_dblp):
        dblp_id = "conf/test"
        self.dblp_context.cache_dblp_id(dblp_id, "content")

        response_text = self.dblp_context.request_or_load_dblp(dblp_id)

        mock_request_dblp.assert_not_called()
        self.assertEqual(response_text, "content")

    @patch("eventseries.src.main.dblp.dblp_context.DblpContext.request_dblp",
           return_value="requested content")
    def test_request_or_load_dblp_requested(self, mock_request_dblp):
        dblp_id = "conf/test"

        response_text = self.dblp_context.request_or_load_dblp(dblp_id)

        self.assertEqual(response_text, "requested content")
        mock_request_dblp.assert_called_once_with(
            dblp_url=self.dblp_context.base_url + dblp_id
        )

    @patch("eventseries.src.main.dblp.dblp_context.DblpContext.store_cache")
    def test_store_on_delete(self, mocked_store):
        dblp_context = DblpContext(cache_file_path=self.test_cache_path,
                                   load_cache=False, store_on_delete=True)
        dblp_context.cache_dblp_id("conf/test1", "content")
        del dblp_context
        mocked_store.assert_called_once()


if __name__ == "__main__":
    unittest.main()
