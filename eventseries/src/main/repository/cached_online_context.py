import abc
import json
import pickle
from pathlib import Path
from typing import Dict, TypeVar, Generic

from eventseries.src.main.repository.completions import EnhancedJSONEncoder

T = TypeVar("T")


class CachedContext(Generic[T]):
    def __init__(self, resource_dir: Path, load_on_init: bool = True, store_on_delete: bool = True):
        self.resource_dir: Path = resource_dir
        self.store_on_delete = store_on_delete
        self.cache: Dict[str, T] = {}

        if load_on_init is None or store_on_delete is None:
            raise ValueError("At least one parameter was None.")
        if not isinstance(self.resource_dir, Path):
            raise TypeError("resource_dir must be a Path object but was " + str(type(resource_dir)))
        if not self.resource_dir.exists():
            raise ValueError("resource_dir does not exist: " + str(resource_dir))
        if not resource_dir.is_dir():
            raise ValueError("resource_dir must be a directory" + str(resource_dir))

        if load_on_init:
            self.load_cached()

    def is_cached(self, key: str) -> bool:
        return key in self.cache

    def _assert_is_cached(self, key: str):
        if not self.is_cached(key):
            raise ValueError("Key is not stored in cache: " + key)

    def load_cached(self):
        file_dictionary = {}
        file_path: Path
        for file_path in self.resource_dir.iterdir():
            if file_path.is_file():
                self.load_cached_file(file_dictionary, file_path)

        self.cache.update(file_dictionary)

    @abc.abstractmethod
    def load_cached_file(self, build_dict: Dict[str, T], file_path: Path):
        pass

    def store_cached(self, overwrite=False):
        for file_name, file_content in self.cache.items():
            file_path = self.resource_dir / file_name
            self.store_content_to_file(file_path, file_content, overwrite)

    @abc.abstractmethod
    def store_content_to_file(self, file_path: Path, file_content, overwrite: bool):
        pass

    def get_cached(self, key: str) -> T:
        self._assert_is_cached(key)
        return self.cache.get(key)

    def cache_content(self, key: str, content: T):
        self.cache[key] = content

    def __del__(self):
        if hasattr(self, "store_on_delete"):
            if not self.store_on_delete:
                return
            if hasattr(self, "cache") and hasattr(self, "resource_dir"):
                self.store_cached()
            else:
                print(
                    "Failed to store cache. Did not found attribute: "
                    f"cache = {hasattr(self, 'cache')} "
                    f"resource_dir = {hasattr(self, 'resource_dir')}"
                )

    @staticmethod
    def _store_pickle(file_path: Path, content, overwrite: bool):
        full_file = file_path.with_suffix(".pickle")
        if not full_file.exists() or overwrite:
            full_file.parent.mkdir(parents=True, exist_ok=True)

            with open(str(full_file), mode="wb") as file:
                pickle.dump(obj=content, file=file)

    @staticmethod
    def _store_json(content, file_path: Path, overwrite: bool):
        full_file = file_path.with_suffix(".json")
        if not full_file.exists() or overwrite:
            full_file.parent.mkdir(parents=True, exist_ok=True)
            with full_file.open(mode="w") as file:
                file.write(json.dumps(content, cls=EnhancedJSONEncoder))

    @staticmethod
    def _load_json(file_path: Path):
        with file_path.open("r") as file:
            return json.loads(file.read())

    @staticmethod
    def _load_pickle(file_path: Path):
        with file_path.open("rb") as file:
            return pickle.loads(file.read())
