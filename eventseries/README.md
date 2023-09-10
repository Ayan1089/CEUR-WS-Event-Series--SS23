### Pipeline Execution

- The complete pipeline is executed by `main.py`.
- The pipeline consists of multiple steps:
1. Query Wikidata for the newest information (within the `repository`)
2. Patch wrong information, like malformed dblp ids
3. Complete Wikidata entries
    - e.g. acronyms, ordinal values etc.
4. Execute the `DblpMatcher` and `FullMatcher`
    - Use the found matches and test and training set for the other algorithms
5. Filter events that don't have *part of a series* present and no found matches
6. Execute NLP-based algorithms
7. Cross validate the found matches

#### Further Information
- `build.yml` will execute `main.py` on every commit and pull request
  - Due to the long runtime this should only be done for critical updates to *main* or reduced to subsets of matcher
- Executing the whole pipeline can take up to 1h
  - The runtime is completely dominated by the *word2vec* matcher
  - Without it (using the dblp-zip) the runtime will be at most a couple of minutes
- `ImportError: sys.meta_path is None, Python is likely shutting down`
  - If you get this error at the end of tests or after aborting a run this likely to `CachedContext` saving files in `__del__` while critical python functionality already shut down
- Executing *word2vec* typically gives some warnings which we couln't totally locate and does not seem to impact the result


