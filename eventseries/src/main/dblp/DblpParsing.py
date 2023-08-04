import itertools
import pickle
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union, List

from bs4 import Tag, BeautifulSoup

from eventseries.src.main.dblp.EventClasses import Event, DblpEvent, EventSeries
from eventseries.src.main.dblp.VenueInformationClasses import HasPart
from eventseries.src.main.dblp.VenueInformationClasses import (
    VenueInformation,
    IsPartOf,
    NameWithOptionalReference,
    Predecessor,
    Related,
    Status,
    Successor,
    YearRange,
)
from eventseries.src.main.dblp.dblp import DblpContext


class EventTitleParser:
    @staticmethod
    def extract_virtual_location(full_title: str) -> Optional[Tuple[str, str]]:
        opt_virtual = re.search(r"\[virtual(?: event)?\]", full_title)
        if opt_virtual:
            # strip whitespace left and right of [virtual]
            location = opt_virtual.group()
            return full_title.rstrip().removesuffix(location).rstrip(), location
        return None

    @staticmethod
    def extract_location(full_title: str) -> Tuple[str, Optional[str]]:
        if ":" not in full_title:
            title_with_virtual = EventTitleParser.extract_virtual_location(full_title)
            if title_with_virtual is not None:
                return title_with_virtual
            else:
                print("Missing : in title: " + full_title)
                if full_title.count(";") == 1:
                    print("Found ; in title: " + full_title + " using this instead.")
                    return EventTitleParser.extract_location(
                        full_title.replace(";", ":")
                    )

        event_title_opt_location = full_title.split(":")
        location: Optional[str] = (
            event_title_opt_location[1] if len(event_title_opt_location) > 1 else None
        )
        if location:
            location = location.strip()
        title: str = event_title_opt_location[0].rstrip()
        return title, location

    @staticmethod
    def test_realistic_year(year: int, title: str):
        if year <= 1900 or year > datetime.now().year:
            print(f"Found suspicious year {year} in title {title}")

    @staticmethod
    def extract_year(title: str) -> Optional[int]:
        # test if year is at end
        opt_year = re.search(r"\d{4}$", title)
        if opt_year is None:
            # test if year is somewhere
            opt_year = re.search(r"\d{4}", title)
            if opt_year is not None:
                print(f"Found year {opt_year.group()} but not at end of title: {title}")
        if opt_year is None:
            print("Could not find year in title: " + title)
            return None

        year = int(opt_year.group())
        EventTitleParser.test_realistic_year(year=year, title=title)
        return year

    @staticmethod
    def extract_ordinal(title: str) -> Optional[int]:
        opt_ordinal = re.search(r"^(\d+)(?:rd|nd|th|st|\.)", title)
        if opt_ordinal is None:
            return None
        ordinal = int(opt_ordinal.groups()[0])
        if ordinal < 0 or ordinal > 100:
            print(f"Found suspicious ordinal: {ordinal} in title: {title}")


def event_from_title(full_title: str):
    title, opt_location = EventTitleParser.extract_location(full_title)
    opt_year = EventTitleParser.extract_year(title)
    opt_ordinal = EventTitleParser.extract_ordinal(title)

    return Event(title=title, year=opt_year, location=opt_location, ordinal=opt_ordinal)


def dblp_event_from_tag(headline: Tag, given_dblp_id: Optional[str] = None):
    if not isinstance(headline, Tag) or headline.attrs["id"] != "headline":
        raise ValueError(
            "headline parameter was either not instance of Tag or did not had 'headline' as id"
            + str(headline)
        )

    dblp_id = (
        headline.attrs["data-bhtkey"].removeprefix("db/")
        if given_dblp_id is None
        else given_dblp_id
    )
    event = event_from_title(headline.find("h1").string)
    return DblpEvent(dblp_id=dblp_id, **event.__dict__)


class EventSeriesParser:
    @staticmethod
    def is_event_series(soup: BeautifulSoup):
        #
        breadcrumbs = soup.find(id="breadcrumbs")
        if breadcrumbs is None:
            return False
        try:
            last_breadcrumb: Tag = max(
                breadcrumbs.find_all("span", {"itemprop": "itemListElement"}),
                key=lambda span: int(span.find("meta").attrs["content"]),
            )

            return last_breadcrumb.find("a").attrs["href"] == "https://dblp.org/db/conf"
        except Union[AttributeError, KeyError, ValueError]:
            return False

    @staticmethod
    def parse_event_tag(event_h2: Tag) -> List[Event]:
        full_title = "".join(event_h2.strings)
        title, opt_location = EventTitleParser.extract_location(full_title)
        opt_year = EventTitleParser.extract_year(title)
        title = title.removesuffix(str(opt_year)).rstrip()

        if " / " in title:
            all_events = title.split(" / ")
        else:
            all_events = [title]

        if opt_year:
            all_events = [
                event_title + " " + str(opt_year) for event_title in all_events
            ]

        return [
            Event(
                title=event_title,
                year=opt_year,
                location=opt_location,
                ordinal=EventTitleParser.extract_ordinal(event_title),
            )
            for event_title in all_events
        ]

    @staticmethod
    def extract_dblp_id_from_header_tag(header: Tag):
        dblp_id = None
        if "data-stream" in header.attrs and header.attrs["data-stream"].startswith(
            "conf/"
        ):
            dblp_id = header.attrs["data-stream"]
        if dblp_id is None and "data-bhtkey" in header.attrs:
            opt_dblp_id = (
                header.attrs["data-bhtkey"].removeprefix("db/").removesuffix("/index")
            )
            if opt_dblp_id.startswith("conf/"):
                dblp_id = opt_dblp_id
        if dblp_id is None:
            raise ValueError("Could not extract dblp_id from header" + str(header))
        return dblp_id


def event_series_from_soup(soup: BeautifulSoup, given_dblp_id: Optional[str] = None):
    if not EventSeriesParser.is_event_series(soup):
        raise ValueError(
            "Soup parameter is probably not representing an event series" + str(soup)
        )
    header = soup.find("header", {"id": "headline"})

    # Extract dblp_id
    dblp_id = (
        given_dblp_id
        if given_dblp_id is not None
        else EventSeriesParser.extract_dblp_id_from_header_tag(header)
    )

    headline = header.find("h1")
    name = str(headline.string)
    if "Redirecting" in name:
        print("Suspicious name found: " + name + " for dblp_id: " + dblp_id)
    opt_abbreviation = re.search(r"\((\w{1,20})\)", name)
    if opt_abbreviation is None and re.search(r"\((\w+)\)", name):
        print(
            "Possible abbreviation longer than 20 characters: "
            + re.search(r"\((\w+)\)", name).groups()[0]
        )
    abbreviation = None
    if opt_abbreviation is not None and len(opt_abbreviation.groups()) == 1:
        abbreviation = opt_abbreviation.groups()[0]

    if abbreviation:
        non_word = re.search(r"\W+", abbreviation)
        if non_word is not None:
            print(
                f"Suspicious characters found in abbreviation: '{non_word.group()}'"
                f" Found in series name '{name}'"
            )

    infos = soup.find(id="info-section")
    venue_info = parse_venue_div(infos) if infos is not None else None

    main_div = soup.find(id="main")
    event_h2_list: List[Tag] = [
        header.find("h2")
        for header in main_div.find_all("header", {"class": "h2"}, recursive=False)
    ]
    events: List[Event] = list(
        itertools.chain.from_iterable(
            [EventSeriesParser.parse_event_tag(event_h2) for event_h2 in event_h2_list]
        )
    )

    return EventSeries(
        dblp_id=dblp_id,
        name=name,
        abbreviation=abbreviation,
        venue_information=venue_info,
        mentioned_events=events,
    )


def name_with_opt_reference_from_tag(tag: BeautifulSoup):
    href = tag.find("a")
    if href is None:
        strings = tag.find_all(string=True, recursive=False)
        if not strings:
            raise ValueError(
                "Tag not parseable as NameWithOptionalReference. Could not find strings: "
                + str(tag)
            )
        return NameWithOptionalReference(name="".join(strings).strip())
    else:
        return NameWithOptionalReference(
            name=href.get_text(), reference=href.attrs["href"]
        )


def year_range_from_string(text: str):
    until = None
    since = None
    years = []
    until_year: List[int] = re.findall(r"until (\d{4})", text)
    if until_year:
        until = int(until_year[0])
    since_year: List[int] = re.findall(r"since (\d{4})", text)
    if since_year:
        since = int(since_year[0])
    year_ranges = re.findall(r"(\d{4})-(\d{4})", text)
    if year_ranges:
        for start, stop in year_ranges:
            years += list(range(int(start), int(stop) + 1))
    individual_years = re.findall(
        r"\b(?<!-)(\d{4})(?!\s*-\s*\d{4}\b)", text
    )  # excludes YYYY-YYYY
    if individual_years:
        years += [
            int(y) for y in individual_years if since != int(y) and until != int(y)
        ]  # avoid adding since and until dates
    if not years and since is None and until is None:
        raise ValueError("Could not parse YearRange from " + text)
    return YearRange(years=years, since=since, until=until)


class VenueInformationParser:
    @staticmethod
    def _parse_access(li: BeautifulSoup) -> bool:
        if "some or all publications openly available" not in li.get_text():
            raise ValueError(
                f"Expected 'some or all publications openly available' in {li.get_text()}"
            )
        return True

    @staticmethod
    def _parse_has_part(li: BeautifulSoup) -> HasPart:
        years = None
        em_text = li.find("em").get_text().strip("has part").rstrip(":")
        if "(" in em_text:
            try:
                years = year_range_from_string(em_text)
            except ValueError as e:
                print(e)
        part = name_with_opt_reference_from_tag(li)
        return HasPart(part=part, years=years)

    @staticmethod
    def _parse_is_part_of(li: BeautifulSoup) -> IsPartOf:
        years = None
        em_text = li.find("em").get_text().strip("is part of").rstrip(":")
        if "(" in em_text:
            try:
                years = year_range_from_string(em_text)
            except ValueError as e:
                print(e)
        part = name_with_opt_reference_from_tag(li)
        return IsPartOf(partOf=part, years=years)

    @staticmethod
    def _parse_not_to_be_confused_with(li: BeautifulSoup) -> NameWithOptionalReference:
        return name_with_opt_reference_from_tag(li)

    @staticmethod
    def _parse_predecessor(li: BeautifulSoup) -> Predecessor:
        years = None
        em_text = li.find("em").get_text().strip("predecessor").rstrip(":")
        if "(" in em_text:
            try:
                years = year_range_from_string(em_text)
            except ValueError as e:
                print(e)
        reference = name_with_opt_reference_from_tag(li)
        return Predecessor(reference=reference, year_range=years)

    @staticmethod
    def _parse_related(li: BeautifulSoup) -> Related:
        reference = name_with_opt_reference_from_tag(li)
        em_text = li.find("em").get_text()
        if "(" in em_text:
            meta_info = em_text[em_text.find("(") + 1 : em_text.find(")")]
            return Related(relation_qualifier=meta_info, reference=reference)
        return Related(reference=reference)

    @staticmethod
    def _parse_status(li: BeautifulSoup) -> Status:
        pattern = r"as of (\d{4}), this venue has been discontinued"
        discontinued_years = re.findall(pattern, li.get_text())
        if len(discontinued_years) != 1:
            raise ValueError("Could not find discontinued information")
        return Status(discontinuation_year=int(discontinued_years[0]))

    @staticmethod
    def _parse_successor(li: BeautifulSoup) -> Successor:
        years = None
        merged_into = False
        em_text = li.find("em").get_text().strip("successor").rstrip(":")
        if "(" in em_text:
            if "merged into" in em_text:
                merged_into = True
            else:
                try:
                    years = year_range_from_string(em_text)
                except ValueError as e:
                    print(e)
        reference = name_with_opt_reference_from_tag(li)
        return Successor(reference=reference, year_range=years, merged_into=merged_into)


def parse_venue_div(info_section_div: BeautifulSoup) -> Optional[VenueInformation]:
    if info_section_div.get("id") != "info-section":
        raise ValueError(
            "Argument was not the expected info-section div element "
            + info_section_div.get("id")
        )
    if len(info_section_div.find_all()) == 0:
        return None

    list_entries = info_section_div.find_all("li")

    names = [
        "access",
        "has part",
        "is part of",
        "not to be confused with",
        "predecessor",
        "related",
        "status",
        "successor",
    ]

    grouped = {
        name: [li for li in list_entries if name in li.find("em").string]
        for name in names
    }

    parameter = dict()
    try:
        for name in names:
            with_underscores = name.replace(" ", "_")
            parse_method = getattr(VenueInformationParser, "_parse_" + with_underscores)
            parameter[with_underscores] = [parse_method(li) for li in grouped[name]]

        return VenueInformation(**parameter)
    except Exception as e:
        print(f"Could not parse div: {info_section_div} got exception: {str(e)}")


def store_event_series(
    context: DblpContext,
    path: Path = Path(".") / ".." / "resources" / "dblp_event_series.pickle",
):
    event_series_ids = context.get_cached_series_keys()
    event_series_ids.remove(
        "conf/birthday"
    )  # exclude Festschriften: Birthdays, In Memory of ..., In Honor of ...
    event_series_ids.remove("conf/ac")  # remove advanced courses
    series_contents = [context.get_cached(series) for series in event_series_ids]
    event_series = [
        event_series_from_soup(BeautifulSoup(series, "html.parser"))
        for series in series_contents
    ]
    with open(path, "wb") as file:
        pickle.dump(obj=event_series, file=file)


def load_event_series(
    path: Path = Path(".") / ".." / "resources" / "dblp_event_series.pickle",
) -> List[EventSeries]:
    with open(path, "rb") as file:
        event_series: List[EventSeries] = pickle.load(file)
        return event_series
