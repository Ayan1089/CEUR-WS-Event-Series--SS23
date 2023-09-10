import logging
from typing import List, Dict
from unittest import TestCase
from unittest.mock import patch, AsyncMock

from eventseries.src.main.completion.attribute_completion import (
    complete_ordinals,
    complete_ceurws_titles,
)
from eventseries.src.main.repository.completions import WithOrdinal
from eventseries.src.main.repository.wikidata_dataclasses import (
    WikiDataEvent,
    QID,
    WikiDataProceeding,
)


class TestCompletions(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(level=logging.DEBUG, format="%(name)s %(levelname)s %(message)s")

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
        completions = complete_ceurws_titles(proceedings=[proceeding])
        self.assertEqual(1, len(completions))
        comp = completions[0]
        self.assertEqual(QID("Q113544823"), comp.qid)
        self.assertEqual("Artificial Intelligence and Assistive Medicine", comp.ceurws_title)

    @patch(
        "eventseries.src.main.completion.attribute_completion._title_from_ceurspt",
        AsyncMock(return_value=None),
    )
    def test_complete_ceurws_title_no_ceurspt(self):
        proceeding = WikiDataProceeding(
            qid=QID("Q113544823"),
            label=(
                "Proceedings of the 3rd International Workshop on Artificial Intelligence and"
                " Assistive Medicine"
            ),
            volume_number=1213,
            event=QID("Q113649424"),
        )
        completions = complete_ceurws_titles(proceedings=[proceeding])
        self.assertEqual(1, len(completions))
        comp = completions[0]
        self.assertEqual(QID("Q113544823"), comp.qid)
        self.assertEqual("Artificial Intelligence and Assistive Medicine", comp.ceurws_title)

    def test_bulk_ceurws(self):
        proceedigns = [
            WikiDataProceeding(
                volume_number=1038,
                event=QID(value="Q113649839"),
                qid=QID(value="Q113545018"),
                label="P1",
            ),
            WikiDataProceeding(
                volume_number=173,
                event=QID(value="Q113673512"),
                qid=QID(value="Q113546049"),
                label="P2",
            ),
            WikiDataProceeding(
                volume_number=859,
                event=QID(value="Q113656554"),
                qid=QID(value="Q113545215"),
                label="P3",
            ),
            WikiDataProceeding(
                volume_number=2106,
                event=QID(value="Q48621961"),
                qid=QID(value="Q54499539"),
                label="P4",
            ),
            WikiDataProceeding(
                volume_number=3454,
                event=QID(value="Q121753216"),
                qid=QID(value="Q121753215"),
                label="P5",
            ),
            WikiDataProceeding(
                volume_number=3255,
                event=QID(value="Q115053074"),
                qid=QID(value="Q115053073"),
                label="P6",
            ),
            WikiDataProceeding(
                volume_number=678,
                event=QID(value="Q113672145"),
                qid=QID(value="Q113545413"),
                label="P7",
            ),
            WikiDataProceeding(
                volume_number=1773,
                event=QID(value="Q113637413"),
                qid=QID(value="Q113544131"),
                label="P8",
            ),
            WikiDataProceeding(
                volume_number=155,
                event=QID(value="Q113673481"),
                qid=QID(value="Q113546067"),
                label="P9",
            ),
        ]
        completions = complete_ceurws_titles(proceedigns)
        self.assertEqual(10, len(completions))
