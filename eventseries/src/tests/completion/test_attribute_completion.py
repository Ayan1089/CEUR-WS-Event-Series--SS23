from typing import List, Dict
from unittest import TestCase
from unittest.mock import patch

from eventseries.src.main.completion.attribute_completion import (
    complete_ordinals,
    complete_ceurws_title,
)
from eventseries.src.main.repository.completions import WithOrdinal
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    QID,
    WikiDataProceeding,
)


class TestCompletions(TestCase):
    def test_complete_ordinals(self):
        event1 = WikiDataEvent(
            qid=QID("Q106245681"),
            label=(
                "10th International Workshop on Science Gateways, Edinburgh, Scotland, UK, 13-15"
                " June, 2018"
            ),
            title="10th International Workshop on Science Gateways",
        )
        event2 = WikiDataEvent(
            qid=QID("Q106338504"),
            label=(
                "3rd International Workshop on Science Gateways for Life Sciences, London, United"
                + " Kingdom, June 8-10, 2011"
            ),
        )
        # original data is the third conference, changed to disambiguate form event2
        event3 = WikiDataEvent(
            qid=QID("Q113588872"),
            label="First Conference on Software Engineering and Information Management",
            title="First Conference on Software Engineering and Information Management",
        )
        event4 = WikiDataEvent(
            qid=QID("Q113674219"), label="WOP 2009 - Workshop on Ontology Patterns", ordinal=1
        )
        event_no_ordinal_in_title = WikiDataEvent(
            qid=QID("Q113576246"),
            title="Workshops of the EDBT/ICDT 2022 Joint Conference",
            label="Workshops of the EDBT/ICDT 2022 Joint Conference",
        )

        events = [event1, event2, event3, event4, event_no_ordinal_in_title]

        completions: List[WithOrdinal] = complete_ordinals(events)
        self.assertIsInstance(completions, List)
        # We dont expect a completion for the last two events
        self.assertEqual(3, len(completions))
        by_qid: Dict[QID, WithOrdinal] = {comp.qid: comp for comp in completions}
        self.assertEqual(10, by_qid[QID("Q106245681")].ordinal)
        self.assertEqual(3, by_qid[QID("Q106338504")].ordinal)
        self.assertEqual(1, by_qid[QID("Q113588872")].ordinal)

    def test_complete_ceurws_title(self):
        proceeding = WikiDataProceeding(
            qid=QID("Q113544823"),
            label=(
                "Proceedings of the 3rd International Workshop on Artificial Intelligence and"
                " Assistive Medicine"
            ),
            volume_number=1213,
            event=QID("Q113649424"),
        )
        completions = complete_ceurws_title(proceedings=[proceeding])
        self.assertEqual(1, len(completions))
        comp = completions[0]
        self.assertEqual(QID("Q113544823"), comp.qid)
        self.assertEqual(
            "Artificial Intelligence and Assistive Medicine",
            comp.ceurws_title,
        )

    @patch("eventseries.src.main.completion.attribute_completion._get_title_from_response")
    def test_complete_ceurws_title_no_ceurspt(self, mock_obj):
        mock_obj.return_value = None

        proceeding = WikiDataProceeding(
            qid=QID("Q113544823"),
            label=(
                "Proceedings of the 3rd International Workshop on Artificial Intelligence and"
                " Assistive Medicine"
            ),
            volume_number=1213,
            event=QID("Q113649424"),
        )
        completions = complete_ceurws_title(proceedings=[proceeding])
        self.assertEqual(1, len(completions))
        comp = completions[0]
        self.assertEqual(QID("Q113544823"), comp.qid)
        self.assertEqual(
            "Artificial Intelligence and Assistive Medicine",
            comp.ceurws_title,
        )
