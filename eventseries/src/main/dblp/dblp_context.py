import time
from importlib import resources as ires
from pathlib import Path
from typing import Dict, List, Optional

import requests
import validators


def is_likely_dblp_id(dblp_id: str) -> bool:
    return isinstance(dblp_id, str) and dblp_id.startswith("conf/")


def is_likely_dblp_event(dblp_event_id: str) -> bool:
    return is_likely_dblp_id(dblp_id=dblp_event_id) and dblp_event_id.count("/") == 2


def is_likely_dblp_event_series(dblp_event_series_id: str) -> bool:
    return is_likely_dblp_id(dblp_id=dblp_event_series_id) and dblp_event_series_id.count("/") == 1


class DblpContext:
    """Encapsulates access to dblp events and event-series.
    Accessed sites are cached and can be accessed later.
    Everything is indexed based on the dblp-id (e.g. conf/aaai/affcon2019)."""

    def __init__(
        self,
        dblp_base: str = "https://dblp.org/db/",
        cache_file_path: Path = ires.files("eventseries.src.main") / "resources" / "dblp" / "conf",
        load_cache: bool = True,
        store_on_delete: bool = False,
        dblp_timeout_ns: int = 500_000_000  # half a second in nanoseconds
    ) -> None:
        if (
            dblp_base is None
            or cache_file_path is None
            or load_cache is None
            or store_on_delete is None
        ):
            raise ValueError("At least one parameter was None")
        if not validators.url(dblp_base):
            raise ValueError("dblp_base must be a valid url")
        # Validate cache_file_path
        if not isinstance(cache_file_path, Path):
            raise TypeError("cache_file_path must be a Path object")
        if not cache_file_path.exists():
            raise ValueError("cache_file_path does not exist")
        if not cache_file_path.is_dir():
            raise ValueError("cache_file_path must be a directory")

        self.base_url: str = dblp_base
        self.dblp_cache: Dict[str, str] = {}  # 'dblp_id' : website content
        self.store_on_delete: bool = store_on_delete
        self.dblp_conf_path: Path = cache_file_path
        self.dblp_base_path: Path = cache_file_path.parent
        self.dblp_timeout_ns: int = dblp_timeout_ns
        self.last_request_time_ns: Optional[int] = None  # Time in ns at last request to dblp
        if load_cache:
            self.load_cache()

    @staticmethod
    def _validate_and_clean_dblp_id(dblp_id: str) -> str:
        if dblp_id.startswith("https") or dblp_id.endswith(".html"):
            raise ValueError("dblp_id seems to be an url: " + dblp_id)
        if len(dblp_id) > 100:
            print("dblp_id seems unusually long: " + dblp_id)

        return dblp_id.removesuffix("/")

    def get_cached(self, dblp_id: str) -> str:
        """Access the cached websites by id. Use is_cached before to avoid exceptions.
        :param dblp_id: the requested id
        :return: the stored HTML representation as string
        :raises KeyError if there is nothing stored for this id.
        """
        self._assert_is_cached(dblp_id)
        cleaned_id = DblpContext._validate_and_clean_dblp_id(dblp_id)
        return self.dblp_cache[cleaned_id]

    def cache_dblp_id(self, dblp_id: str, content: str):
        """
        Store the websites html given as string under the id.
        :param dblp_id: The id under which the content should be stored.
        :param content: The websites html as string.
        """
        cleaned_id = DblpContext._validate_and_clean_dblp_id(dblp_id)
        if cleaned_id in self.dblp_cache:
            print("Warning! Overriding cached content: " + dblp_id)
        self.dblp_cache[cleaned_id] = content

    def is_cached(self, dblp_id: str) -> bool:
        """Check whether the id is stored in the cache."""
        cleaned_id = DblpContext._validate_and_clean_dblp_id(dblp_id)
        return cleaned_id in self.dblp_cache

    def _assert_is_cached(self, key: str):
        if not self.is_cached(key):
            raise ValueError("Id is not stored in cache: " + key)

    def load_cache(self):
        if not self.dblp_conf_path.is_dir() or not self.dblp_conf_path.exists():
            print(
                f"Either {str(self.dblp_conf_path)} doesnt exist or is not a directory."
                " Creating empty dict."
            )
            if self.dblp_cache is None:
                self.dblp_cache = {}
            return

        file_dictionary = {}
        file_path: Path
        for file_path in self.dblp_conf_path.rglob("*.html"):
            if file_path.is_file():
                with file_path.open() as file:
                    path = file_path.relative_to(self.dblp_base_path)
                    dblp_id = path.parent / path.stem
                    file_dictionary[str(dblp_id)] = file.read()

        self.dblp_cache.update(file_dictionary)
        print(f"Loaded dblp cache. Found {len(self.dblp_cache)} entries.")

    def store_cache(self, overwrite=False):
        if not self.dblp_conf_path.is_dir():
            raise ValueError("The provided path is not a directory.")

        for file_name, file_content in self.dblp_cache.items():
            file_path = self.dblp_base_path / file_name
            full_file = file_path.with_suffix(".html")
            if not full_file.exists() or overwrite:
                full_file.parent.mkdir(parents=True, exist_ok=True)
                with full_file.open(mode="w") as file:
                    file.write(file_content)

    def __del__(self):
        if hasattr(self, "store_on_delete"):
            if not self.store_on_delete:
                return
            if hasattr(self, "dblp_cache") and hasattr(self, "dblp_conf_path"):
                self.store_cache()
            else:
                print(
                    "Failed to store cache. Did not found attribute: "
                    f"dblp_cache = {hasattr(self, 'dblp_cache')} "
                    f"dblp_file_path = {hasattr(self, 'dblp_file_path')}"
                )

    def request_dblp(self, dblp_url: str, retry: bool = True, ignore_timeout: bool = False) -> str:
        if self.last_request_time_ns is not None and not ignore_timeout:
            diff = time.time_ns() - self.last_request_time_ns
            if diff < self.dblp_timeout_ns:
                time.sleep(diff / 1e9)  # nanoseconds remaining converted to seconds
        response = requests.get(dblp_url, timeout=120)  # wait to minutes max
        self.last_request_time_ns = time.time_ns()
        if response.status_code == 429:
            retry_time = response.headers.get("Retry-After")
            error_msg = "Too many requests to dblp.org"
            if retry and retry_time is not None:
                print(f"{error_msg} Waiting for {retry_time}s before retrying.")
                time.sleep(int(retry_time))
                return self.request_dblp(dblp_url, retry, ignore_timeout=ignore_timeout)
            # failed request without retrying
            raise ValueError(error_msg)
        if response.status_code != 200:
            raise ValueError(f"Failed to request {dblp_url} with code {response.status_code}.")

        return response.text

    def request_or_load_dblp(
        self,
        dblp_db_entry: str,
        ignore_cache: bool = False,
        wait_time: Optional[float] = None,
        **kwargs,
    ):
        if (
            not ignore_cache
            and self.is_cached(dblp_db_entry)
            and self.get_cached(dblp_db_entry) != ""
        ):
            return self.get_cached(dblp_db_entry)
        # Couldn't find id in cache -> requesting it:
        response_text = self.request_dblp(dblp_url=self.base_url + dblp_db_entry, **kwargs)
        if wait_time is not None and wait_time > 0.0:  # Avoid DDOSing dblp
            time.sleep(wait_time)
        if not ignore_cache:
            self.cache_dblp_id(dblp_db_entry, response_text)
        return response_text

    def get_cached_series_keys(self) -> List[str]:
        return [key for key in self.dblp_cache if is_likely_dblp_event_series(key)]

    def get_events_for_series(self, series_id: str) -> List[str]:
        """
        Retrieve all events that are part of the series (have the series_id as prefix).
        :param series_id: The id of the series (e.g. "conf/aaai").
        :return: A list of event-id's that start with the series_id.
        :raises ValueError if the series_id is not part of the cache.
        """
        self._assert_is_cached(series_id)
        return [
            key
            for key in self.dblp_cache
            if is_likely_dblp_event(key) and key.startswith(series_id)
        ]

    def get_series_with_events(
        self, series_ids: Optional[List[str]] = None
    ) -> Dict[str, List[str]]:
        """
        Map every id in series_ids to all events that are part of the series.
        The values for a key are all event-ids that start with the series-id.
        :param series_ids: The list of ids for which events should be matched.
        If none all cached series ids will be used.
        :return: Every series_id to a list of event_id's that have the series as prefix.
        """
        series_keys = self.get_cached_series_keys() if series_ids is None else series_ids
        return {key: self.get_events_for_series(key) for key in series_keys}
