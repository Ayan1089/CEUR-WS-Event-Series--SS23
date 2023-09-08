from importlib import resources as ires
from pathlib import Path
from typing import List, Dict

from eventseries.src.main.repository.cached_online_context import CachedContext
from eventseries.src.main.repository.completions import WikidataItemCompletion, Match
from eventseries.src.main.repository.wikidata_dataclasses import QID


class CompletionCache(CachedContext):
    MATCHES = "matches"
    ITEM_COMPLETION = "wikidata_item_completions"

    def __init__(
            self,
            resource_dir: Path = ires.files("eventseries.src.main") / "resources" / "completions",
            load_on_init: bool = True,
            store_on_delete: bool = True,
    ):
        super().__init__(resource_dir, load_on_init, store_on_delete)
        self.matches: List[Match] = self.cache.get(CompletionCache.MATCHES, [])
        self.event_completions: Dict[QID, List[WikidataItemCompletion]] = self.cache.get(
            CompletionCache.ITEM_COMPLETION, {}
        )
        self.cache[CompletionCache.MATCHES] = self.matches
        self.cache[CompletionCache.ITEM_COMPLETION] = self.event_completions

    def load_cached(self):
        super().load_cached()
        self.matches = self.cache.get(CompletionCache.MATCHES, [])
        self.event_completions = self.cache.get(CompletionCache.ITEM_COMPLETION, {})

    def load_cached_file(self, build_dict, file_path: Path):
        if file_path.stem in (CompletionCache.MATCHES, CompletionCache.ITEM_COMPLETION):
            build_dict[file_path.stem] = CachedContext._load_pickle(file_path)
        else:
            build_dict[file_path.stem] = CachedContext._load_json(file_path)

    def store_content_to_file(self, file_path, file_content, overwrite: bool):
        if file_path.stem in (CompletionCache.MATCHES, CompletionCache.ITEM_COMPLETION):
            super()._store_pickle(file_path, file_content, overwrite)
        else:
            super()._store_json(file_content, file_path, overwrite)

    def get_completions_for_qid(self, qid: QID) -> List[WikidataItemCompletion]:
        completions = self.cache[CompletionCache.ITEM_COMPLETION]
        if qid in completions:
            return completions[qid]
        return []

    def add_completion(self, completion: WikidataItemCompletion):
        completions = self.cache[CompletionCache.ITEM_COMPLETION]
        if completion.qid in completions:
            completions[completion.qid].append(completion)
        else:
            completions[completion.qid] = [completion]

    def get_all_matches(self) -> List[Match]:
        """Return a copy of all matches stored by this cache."""
        return self.matches.copy()

    def get_matches_by_source(self):
        """Return a dictionary of matches with the found_by attribute as key set.
        This allows to group the matches by which algorithm claimed it."""
        return {match.found_by: match for match in self.get_all_matches()}

    def add_match(self, match: Match):
        """Add a match to this cache. NOTE: There is no check for duplicates."""
        self.cache[CompletionCache.MATCHES].append(match)
