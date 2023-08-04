import time
from pathlib import Path
from typing import Optional, List

import requests


class DblpContext:
    def __init__(
        self,
        dblp_base: str = "https://dblp.org/db/",
        cache_file_path: Path = Path("..") / "resources" / "dblp" / "conf",
        load_cache: bool = True,
        store_on_delete: bool = False,
    ) -> None:
        self.base_url: str = dblp_base
        self.dblp_cache: dict = dict()  # 'dblp_id' : website content
        self.store_on_delete = store_on_delete
        self.dblp_conf_path = cache_file_path
        self.dblp_base_path = cache_file_path.parent
        if load_cache:
            self.load_cache()

    @staticmethod
    def _validate_and_clean_dblp_id(dblp_id: str):
        if dblp_id.startswith("https") or dblp_id.endswith(".html"):
            raise ValueError("dblp_id seems to be an url: " + dblp_id)
        if len(dblp_id) > 100:
            print("dblp_id seems unusually long: " + dblp_id)

        return dblp_id.removesuffix("/")

    def get_cached(self, dblp_id: str):
        cleaned_id = DblpContext._validate_and_clean_dblp_id(dblp_id)
        return self.dblp_cache[cleaned_id]

    def cache_dblp_id(self, dblp_id: str, content: str):
        cleaned_id = DblpContext._validate_and_clean_dblp_id(dblp_id)
        if cleaned_id in self.dblp_cache:
            print("Warning! Overriding cached content: " + dblp_id)
        self.dblp_cache[cleaned_id] = content

    def is_cached(self, dblp_id: str):
        cleaned_id = DblpContext._validate_and_clean_dblp_id(dblp_id)
        return cleaned_id in self.dblp_cache

    def load_cache(self):
        if not self.dblp_conf_path.is_dir() or not self.dblp_conf_path.exists():
            print(
                f"Either {str(self.dblp_conf_path)} doesnt exist or is not a directory."
                f" Creating empty dict."
            )
            if self.dblp_cache is None:
                self.dblp_cache = dict()
            return

        file_dictionary = {}
        file: Path
        for file in self.dblp_conf_path.rglob("*.html"):
            if file.is_file():
                with file.open() as f:
                    p = file.relative_to(self.dblp_base_path)
                    dblp_id = p.parent / p.stem
                    file_dictionary[str(dblp_id)] = f.read()

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
                with full_file.open(mode="w") as f:
                    f.write(file_content)

    def __del__(self):
        if hasattr(self, "store_on_delete"):
            if not self.store_on_delete:
                return
            if hasattr(self, "dblp_cache") and hasattr(self, "dblp_conf_path"):
                self.store_cache()
            else:
                print(
                    f"Failed to store cache. Did not found attribute: "
                    f"dblp_cache = {hasattr(self, 'dblp_cache')} "
                    f"dblp_file_path = {hasattr(self, 'dblp_file_path')}"
                )

    @staticmethod
    def request_dblp(dblp_url: str, retry: bool = True):
        response = requests.get(dblp_url)
        if response.status_code == 429:
            retry_time = response.headers.get("Retry-After")
            error_msg = "Too many requests to dblp.org"
            if retry:
                print(f"{error_msg} Waiting for {retry_time}s before retrying.")
                time.sleep(int(retry_time))
                return DblpContext.request_dblp(dblp_url, retry)
            else:
                raise ValueError(error_msg)
        elif response.status_code != 200:
            raise ValueError(
                f"Failed to request {dblp_url} with code {response.status_code}."
            )
        else:
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
        else:
            response_text = DblpContext.request_dblp(
                dblp_url=self.base_url + dblp_db_entry, **kwargs
            )
            if wait_time is not None and wait_time > 0.0:
                time.sleep(wait_time)
        if not ignore_cache:
            self.cache_dblp_id(dblp_db_entry, response_text)
        return response_text

    def get_cached_series_keys(self):
        return [
            key
            for key in self.dblp_cache.keys()
            if key.count("/") == 1 and key.startswith("conf/")
        ]

    def get_events_for_series(self, series_id: str):
        if not self.is_cached(series_id):
            raise ValueError("Series id is not stored in cache: " + series_id)
        return [key for key in self.dblp_cache.keys() if key.startswith(series_id)]

    def get_series_with_events(self, series_ids: Optional[List[str]] = None):
        series_keys = (
            self.get_cached_series_keys() if series_ids is None else series_ids
        )
        return {key: self.get_events_for_series(key) for key in series_keys}
