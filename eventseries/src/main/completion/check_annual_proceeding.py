import os
import re

import nltk
from nltk.corpus import wordnet


class CheckAnnualProceeding:
    def __init__(self) -> None:
        # Download WordNet data
        wordnet_cache_path = os.path.abspath("resources/corpora")
        nltk.data.path.append(wordnet_cache_path)
        if not os.path.exists(os.path.join(wordnet_cache_path, "corpora/wordnet.zip")):
            nltk.download("wordnet", download_dir=wordnet_cache_path)

    def is_proceeding_annual(self, title: str) -> bool:
        # Get synonyms of "annual"
        synonyms_of_annual = self.get_synonyms("annual")
        return self.contains_synonym(title, synonyms_of_annual)

    def get_synonyms(self, word: str) -> list:
        synonyms = []
        for syn in wordnet.synsets(word):
            for lemma in syn.lemmas():
                synonyms.append(lemma.name())
        return synonyms

    def contains_synonym(self, text: str, synonyms: list) -> bool:
        pattern = r"\b(?:" + "|".join(synonyms) + r")\b"
        return bool(re.search(pattern, text, flags=re.IGNORECASE))
