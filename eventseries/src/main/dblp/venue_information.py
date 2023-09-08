from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


class YearRange:
    def __init__(
        self, years: List[int], since: Optional[int] = None, until: Optional[int] = None
    ):
        if not years and since is None and until is None:
            raise ValueError("At least one parameter has to be given.")

        self.since: Optional[int] = since
        self.until: Optional[int] = until
        self.years: List[int] = years

    def __contains__(self, item):
        if not isinstance(item, int):
            raise ValueError("Can only contain ints")

        if self.since is not None and self.since > item:
            return False
        if self.until is not None and self.until < item:
            return False
        if self.years:
            return item in self.years
        return True


@dataclass
class NameWithOptionalReference:
    name: str
    reference: Optional[str] = None  # url to the referenced item


@dataclass
class Status:
    discontinuation_year: int


@dataclass
class HasPart:
    part: NameWithOptionalReference
    years: Optional[YearRange] = None


@dataclass
class IsPartOf:
    partOf: NameWithOptionalReference
    years: Optional[YearRange] = None


@dataclass
class Related:
    reference: NameWithOptionalReference
    relation_qualifier: Optional[str] = None


@dataclass
class Successor:
    reference: NameWithOptionalReference
    year_range: Optional[YearRange] = None
    merged_into: bool = False


@dataclass
class Predecessor:
    reference: NameWithOptionalReference
    year_range: Optional[YearRange] = None


@dataclass
class VenueInformation:
    access: List[bool]
    has_part: List[HasPart]
    is_part_of: List[IsPartOf]
    not_to_be_confused_with: List[NameWithOptionalReference]
    predecessor: List[Predecessor]
    related: List[Related]
    status: List[Status]
    successor: List[Successor]
