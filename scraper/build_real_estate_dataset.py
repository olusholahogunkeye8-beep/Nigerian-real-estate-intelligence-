from __future__ import annotations

import csv
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from statistics import median


BASE_DIR = Path("/Users/kelvinanowu/GROUP-PROJECTS")
RAW_DIR = BASE_DIR / "raw dataset-3sites"

CLEANED_OUTPUT = BASE_DIR / "nigeria_real_estate_cleaned.csv"
MANUAL_REVIEW_OUTPUT = BASE_DIR / "nigeria_real_estate_manual_review.csv"
ANALYSIS_READY_OUTPUT = BASE_DIR / "nigeria_real_estate_analysis_ready.csv"
STATE_ANALYSIS_OUTPUT = BASE_DIR / "nigeria_real_estate_state_analysis.csv"
SUMMARY_OUTPUT = BASE_DIR / "nigeria_real_estate_cleaning_summary.json"

VALID_STATES = {
    "Abuja",
    "Abia",
    "Adamawa",
    "Akwa Ibom",
    "Anambra",
    "Bauchi",
    "Bayelsa",
    "Benue",
    "Borno",
    "Cross River",
    "Delta",
    "Ebonyi",
    "Edo",
    "Ekiti",
    "Enugu",
    "Gombe",
    "Imo",
    "Jigawa",
    "Kaduna",
    "Kano",
    "Katsina",
    "Kebbi",
    "Kogi",
    "Kwara",
    "Lagos",
    "Nasarawa",
    "Niger",
    "Ogun",
    "Ondo",
    "Osun",
    "Oyo",
    "Plateau",
    "Rivers",
    "Sokoto",
    "Taraba",
    "Yobe",
    "Zamfara",
}

DATE_FORMATS = ("%d %b %Y", "%Y-%m-%d", "%d/%m/%Y")

CLEANED_FIELDS = [
    "title",
    "location",
    "area_bucket",
    "property_type",
    "price",
    "bedrooms",
    "bathrooms",
    "added_date",
    "updated_date",
    "month_posted",
    "state",
    "price_category",
]

AREA_BUCKETS = {
    "Lagos": {
        "Lekki": [
            "Lekki", "Lekki Phase 1", "Lekki Phase 2", "Ikate", "Ikota", "Chevron",
            "Agungi", "Osapa", "Jakande", "Ibeju Lekki", "Orchid", "Ologolo", "VGC",
        ],
        "Ajah": ["Ajah", "Sangotedo", "Abijo", "Awoyaya"],
        "Victoria Island": ["Victoria Island (VI)", "Victoria Island", "VI", "Eko Atlantic City", "Oniru"],
        "Ikoyi": ["Ikoyi", "Banana Island"],
        "Ikeja": ["Ikeja", "Alausa", "Opebi", "Maryland", "Magodo", "Ojodu", "Ogba", "Adeniyi Jones"],
        "Mainland": ["Yaba", "Surulere", "Gbagada", "Shomolu", "Ketu", "Ogudu", "Ojota", "Somolu"],
        "Alimosho Axis": ["Alimosho", "Ikotun", "Ipaja", "Egbe", "Abule Egba", "Agege", "Command", "Fagba", "Alagbado"],
        "Isolo Axis": ["Isolo", "Ajao Estate", "Ago Palace", "Bucknor", "Ijegun"],
        "Ikorodu": ["Ikorodu", "Igbogbo"],
        "Apapa": ["Apapa"],
        "Lagos Island": ["Lagos Island", "Tinubu Square"],
        "Festac/Ojo Axis": ["Festac", "Amuwo Odofin", "Satellite Town", "Ojo", "Orile"],
        "Kosofe Axis": ["Kosofe", "Ikosi", "CMD Road", "Ilupeju", "Palm Grove"],
        "Isheri/OPIC Axis": ["Isheri North", "Opic", "Agbara-Igbesa"],
        "Others": [],
    },
    "Abuja": {
        "Maitama": ["Maitama District", "Maitama", "Main Maitama", "Maitama 2"],
        "Asokoro": ["Asokoro District", "Asokoro", "Admiralty Estate"],
        "Guzape": ["Guzape District", "Guzape", "Guzape Main"],
        "Katampe": ["Katampe"],
        "Wuye": ["Wuye"],
        "Jahi": ["Jahi"],
        "Gwarinpa": ["Gwarinpa"],
        "Lokogoma": ["Lokogoma District", "Lokogoma"],
        "Lugbe": ["Lugbe District", "Lugbe", "Airport Road"],
        "Galadimawa": ["Galadimawa"],
        "Kubwa": ["Kubwa"],
        "Apo": ["Apo"],
        "Life Camp": ["Life Camp"],
        "Wuse": ["Wuse", "Wuse 2"],
        "Jabi": ["Jabi"],
        "Mabushi": ["Mabushi"],
        "Garki": ["Garki", "Area 11"],
        "Kaura": ["Kaura", "Games Village"],
        "Utako": ["Utako"],
        "Kado": ["Kado"],
        "Karsana": ["Karsana", "Karsana East"],
        "Dape": ["Dape"],
        "Mbora": ["Mbora", "Nbora"],
        "Durumi": ["Durumi"],
        "Idu": ["Idu", "Idu Industrial"],
        "Mpape": ["Mpape"],
        "Kukwaba": ["Kukwaba"],
        "Karmo": ["Karmo"],
        "Kuje": ["Kuje"],
        "Dawaki": ["Dawaki"],
        "Karu Axis": ["Jikwoyi", "Karu", "Karshi", "Duboyi"],
        "Gaduwa Axis": ["Gaduwa", "Gudu", "Wumba", "Dakwo", "Dakibiyu", "Sunny Vale", "Sunnyvale"],
        "Bwari Axis": ["Bwari", "Ushafa"],
        "Others": [],
    },
    "Rivers": {
        "Port Harcourt": ["Port Harcourt", "GRA", "Trans Amadi", "Abuloma"],
        "Obio-Akpor": ["Obio-Akpor", "Rumuodumaya", "Rumuodomaya", "Rupokwu", "Rumuolumeni", "Eliozu", "Ozuoba", "Elelenwo", "Eneka", "Rumuigbo", "Rumuola", "Rumuokoro", "Rumuekeni", "Iwofe"],
        "Others": [],
    },
    "Oyo": {
        "Ibadan": ["Ibadan", "Bodija", "Akobo", "Jericho", "Samonda", "Agodi", "Oluyole", "Elebu", "Akala Express", "Oke Ado", "Apata", "Mokola", "Basorun", "Onireke", "Idishin", "Idi Ishin", "Nihort"],
        "Ogbomoso": ["Ogbomoso"],
        "Iseyin": ["Iseyin"],
        "Saki": ["Saki"],
        "Others": [],
    },
    "Ogun": {
        "Mowe": ["Mowe", "Mowe Ofada", "Ofada"],
        "Ibafo": ["Ibafo", "Asese", "Warewa"],
        "Abeokuta": ["Abeokuta"],
        "Ota": ["Ota", "Ado Odo Ota", "Atan Ota", "Ado-Odo/Ota"],
        "Ifo Axis": ["Akute", "Alagbole", "Ifo", "Ojodu Berger"],
        "Obafemi Owode": ["Obafemi Owode", "Opic", "Isheri North"],
        "Odeda": ["Odeda", "Alabata", "Funaab"],
        "Ijebu Axis": ["Ijebu"],
        "Ogun Waterside": ["Ogun Waterside"],
        "Others": [],
    },
    "Delta": {
        "Asaba": ["Asaba", "Ibusa", "Issele Azagba", "Ubulu Okiti", "Osele Uku"],
        "Warri": ["Warri", "Okpe", "Egberode"],
        "Others": [],
    },
    "Enugu": {
        "Enugu": ["Enugu"],
        "Independence Layout": ["Independence Layout", "Independent Layout", "Wtc"],
        "Trans Ekulu": ["Trans Ekulu"],
        "GRA": ["GRA", "Gra", "Old Gra", "New Gra", "Golf Annex", "Golf Estate"],
        "Thinkers Corner": ["Thinkers Corner"],
        "Emene": ["Emene"],
        "Centenary City": ["Centenary City", "Cetenary City"],
        "New Haven": ["New Heaven", "New Haven", "New Heaven Extension", "New Haven Extension"],
        "Others": [],
    },
    "Imo": {
        "Owerri": ["Owerri"],
        "New Owerri": ["New Owerri", "Owerri Municipal", "Housing Area"],
        "Avu": ["Avu", "Umuguma", "World Bank", "Amakabo", "Umoku Avu", "Obosima"],
        "Others": [],
    },
    "Kaduna": {
        "Kaduna North": ["Kaduna North", "Ungwan Dosa", "Ungwan Rimi", "Angwan Rimi", "Malali", "Kabala"],
        "Kaduna South": ["Kaduna South", "Barnawa", "Sabon Tasha"],
        "Chikun": ["Chikun", "Highcost", "High Cost"],
        "Others": [],
    },
    "Kano": {
        "Kano Municipal": ["Kano Municipal"],
        "Nasarawa": ["Nasarawa"],
        "Zaria Road": ["Zaria Road"],
        "Others": [],
    },
}

MANUAL_REVIEW_FIELDS = [
    "source_file",
    "source_row",
    "review_reason",
    "title",
    "location",
    "property_type_raw",
    "price_raw",
    "bedrooms_raw",
    "bathrooms_raw",
    "added_date_raw",
    "updated_date_raw",
]


@dataclass
class Listing:
    source_file: str
    source_row: int
    title: str
    location: str
    area_bucket: str
    property_type: str
    price: int
    bedrooms: int | None
    bathrooms: int | None
    added_date: str
    updated_date: str
    month_posted: str
    state: str
    title_norm: str
    location_norm: str
    dedupe_title: str


def normalize_unicode(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def clean_text(text: str) -> str:
    text = normalize_unicode(text)
    text = text.replace("✈️", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_for_match(text: str) -> str:
    text = clean_text(text).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_price(value: str) -> int | None:
    cleaned = clean_text(value).replace("₦", "").replace(",", "").replace(" ", "")
    if not cleaned:
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    if number < 10:
        return None
    return int(round(number))


def parse_intish(value: str) -> int | None:
    cleaned = clean_text(value)
    if not cleaned:
        return None
    try:
        number = float(cleaned.replace(",", ""))
    except ValueError:
        return None
    if number <= 0:
        return None
    return int(round(number))


def infer_bedrooms(title: str, property_type_raw: str) -> int | None:
    text = f"{title} {property_type_raw}".lower()
    match = re.search(r"(\d+)\s*bed", text)
    if match:
        return int(match.group(1))
    return None


def parse_date(value: str) -> datetime | None:
    cleaned = clean_text(value)
    if not cleaned:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def format_date(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%d/%m/%Y")


def derive_state(location: str) -> str | None:
    normalized = clean_text(location)
    lowered = normalize_for_match(normalized)
    tokens = lowered.split()
    if len(tokens) >= 2:
        suffix2 = " ".join(tokens[-2:])
        for state in VALID_STATES:
            if normalize_for_match(state) == suffix2:
                return state
    if tokens:
        suffix1 = tokens[-1]
        for state in VALID_STATES:
            if normalize_for_match(state) == suffix1:
                return state
    return None


def derive_property_type(title: str, property_type_raw: str) -> str | None:
    text = normalize_for_match(f"{title} {property_type_raw}")

    if any(token in text for token in ("office", "office space")):
        return "Office"
    if any(token in text for token in ("self contain", "self contained", "single room")):
        return "Self Contained"
    if "studio apartment" in text or re.search(r"\bstudio\b", text):
        return "Studio Apartment"
    if "mini flat" in text or "room and parlour" in text:
        return "Mini Flat"
    if "block of flats" in text or "blocks of flats" in text:
        return "Block Of Flats"
    if "penthouse" in text:
        return "Penthouse"
    if "maisonette" in text:
        return "Maisonette"
    if "townhouse" in text or "town house" in text:
        return "Townhouse"
    if "semi detached" in text and "duplex" in text:
        return "Semi-Detached Duplex"
    if ("terraced duplex" in text or "terrace duplex" in text or "terraced house" in text
            or "terrace house" in text):
        return "Terraced Duplex"
    if (("detached duplex" in text or "fully detached" in text or "detached house" in text)
            and "bungalow" not in text):
        return "Detached Duplex"
    if "bungalow" in text:
        return "Bungalow"
    if "duplex" in text:
        return "Duplex"
    if any(token in text for token in ("land", "plot", "sqm", "sqms", "acre")):
        return "Land"
    if any(
        token in text
        for token in (
            "commercial property",
            "hotel",
            "guest house",
            "mall",
            "plaza",
            "factory",
            "warehouse",
            "shop",
            "filling station",
            "restaurant",
            "bar",
            "school",
            "hostel",
        )
    ):
        return "Commercial Property"
    if "flat" in text:
        return "Flat"
    if "apartment" in text:
        return "Apartment"
    if "house" in text:
        return "House"
    return None


def derive_area_bucket(state: str, location: str) -> str:
    state_buckets = AREA_BUCKETS.get(state)
    if not state_buckets:
        return "Others"

    normalized_location = normalize_for_match(location)
    best_bucket = "Others"
    best_length = -1

    for bucket_name, aliases in state_buckets.items():
        for alias in aliases:
            normalized_alias = normalize_for_match(alias)
            if normalized_alias and normalized_alias in normalized_location:
                if len(normalized_alias) > best_length:
                    best_bucket = bucket_name
                    best_length = len(normalized_alias)

    return best_bucket


def build_dedupe_title(title: str) -> str:
    text = normalize_for_match(title)
    text = re.sub(r"\bfor sale\b", " ", text)
    text = re.sub(r"\bnewly built\b", " ", text)
    text = re.sub(r"\btastefully finished\b", " ", text)
    text = re.sub(r"\bluxury\b", " ", text)
    text = re.sub(r"\bbrand new\b", " ", text)
    text = re.sub(r"\bfully furnished\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def quantile(values: list[float], q: float) -> float:
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    idx = (len(values) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def should_drop_for_rent(title: str, property_type_raw: str) -> bool:
    text = normalize_for_match(f"{title} {property_type_raw}")
    return "for rent" in text or text.endswith("rent")


def load_and_standardize() -> tuple[list[Listing], list[dict], Counter]:
    clean_rows: list[Listing] = []
    review_rows: list[dict] = []
    summary = Counter()

    for path in sorted(RAW_DIR.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row_number, row in enumerate(reader, start=2):
                summary["rows_ingested"] += 1

                title = clean_text(row.get("title", ""))
                location = clean_text(row.get("location", ""))
                property_type_raw = clean_text(row.get("property_type", ""))
                price_raw = clean_text(row.get("price", ""))
                bedrooms_raw = clean_text(row.get("bedrooms", ""))
                bathrooms_raw = clean_text(row.get("bathrooms", ""))
                added_date_raw = clean_text(row.get("added_date", ""))
                updated_date_raw = clean_text(row.get("updated_date", ""))

                if should_drop_for_rent(title, property_type_raw):
                    summary["rows_dropped_for_rent"] += 1
                    review_rows.append(
                        {
                            "source_file": path.name,
                            "source_row": row_number,
                            "review_reason": "for_rent_listing",
                            "title": title,
                            "location": location,
                            "property_type_raw": property_type_raw,
                            "price_raw": price_raw,
                            "bedrooms_raw": bedrooms_raw,
                            "bathrooms_raw": bathrooms_raw,
                            "added_date_raw": added_date_raw,
                            "updated_date_raw": updated_date_raw,
                        }
                    )
                    continue

                state = derive_state(location)
                if state is None:
                    summary["rows_sent_to_manual_review_invalid_state"] += 1
                    review_rows.append(
                        {
                            "source_file": path.name,
                            "source_row": row_number,
                            "review_reason": "invalid_state_from_location",
                            "title": title,
                            "location": location,
                            "property_type_raw": property_type_raw,
                            "price_raw": price_raw,
                            "bedrooms_raw": bedrooms_raw,
                            "bathrooms_raw": bathrooms_raw,
                            "added_date_raw": added_date_raw,
                            "updated_date_raw": updated_date_raw,
                        }
                    )
                    continue

                property_type = derive_property_type(title, property_type_raw)
                if property_type is None:
                    summary["rows_sent_to_manual_review_unmapped_property_type"] += 1
                    review_rows.append(
                        {
                            "source_file": path.name,
                            "source_row": row_number,
                            "review_reason": "unmapped_property_type",
                            "title": title,
                            "location": location,
                            "property_type_raw": property_type_raw,
                            "price_raw": price_raw,
                            "bedrooms_raw": bedrooms_raw,
                            "bathrooms_raw": bathrooms_raw,
                            "added_date_raw": added_date_raw,
                            "updated_date_raw": updated_date_raw,
                        }
                    )
                    continue

                price = parse_price(price_raw)
                if price is None:
                    summary["rows_sent_to_manual_review_missing_or_invalid_price"] += 1
                    review_rows.append(
                        {
                            "source_file": path.name,
                            "source_row": row_number,
                            "review_reason": "missing_or_invalid_price",
                            "title": title,
                            "location": location,
                            "property_type_raw": property_type_raw,
                            "price_raw": price_raw,
                            "bedrooms_raw": bedrooms_raw,
                            "bathrooms_raw": bathrooms_raw,
                            "added_date_raw": added_date_raw,
                            "updated_date_raw": updated_date_raw,
                        }
                    )
                    continue

                bedrooms = parse_intish(bedrooms_raw)
                if bedrooms is None:
                    bedrooms = infer_bedrooms(title, property_type_raw)

                bathrooms = parse_intish(bathrooms_raw)

                if property_type == "Land":
                    bedrooms = None
                    bathrooms = None

                added_date = parse_date(added_date_raw)
                updated_date = parse_date(updated_date_raw) or added_date
                if added_date is None:
                    summary["rows_sent_to_manual_review_invalid_added_date"] += 1
                    review_rows.append(
                        {
                            "source_file": path.name,
                            "source_row": row_number,
                            "review_reason": "invalid_added_date",
                            "title": title,
                            "location": location,
                            "property_type_raw": property_type_raw,
                            "price_raw": price_raw,
                            "bedrooms_raw": bedrooms_raw,
                            "bathrooms_raw": bathrooms_raw,
                            "added_date_raw": added_date_raw,
                            "updated_date_raw": updated_date_raw,
                        }
                    )
                    continue

                if updated_date and updated_date < added_date:
                    updated_date = added_date
                    summary["updated_dates_corrected"] += 1

                location_norm = normalize_for_match(location)
                title_norm = normalize_for_match(title)

                clean_rows.append(
                    Listing(
                        source_file=path.name,
                        source_row=row_number,
                        title=title,
                        location=location,
                        area_bucket=derive_area_bucket(state, location),
                        property_type=property_type,
                        price=price,
                        bedrooms=bedrooms,
                        bathrooms=bathrooms,
                        added_date=format_date(added_date),
                        updated_date=format_date(updated_date),
                        month_posted=added_date.strftime("%Y-%m"),
                        state=state,
                        title_norm=title_norm,
                        location_norm=location_norm,
                        dedupe_title=build_dedupe_title(title),
                    )
                )

    return clean_rows, review_rows, summary


def deduplicate(rows: list[Listing]) -> tuple[list[Listing], list[dict], Counter]:
    summary = Counter()
    review_rows: list[dict] = []

    exact_groups: dict[tuple, list[Listing]] = defaultdict(list)
    for row in rows:
        exact_key = (
            row.title_norm,
            row.location_norm,
            row.property_type,
            row.price,
            row.bedrooms,
            row.bathrooms,
        )
        exact_groups[exact_key].append(row)

    exact_kept: list[Listing] = []
    for group in exact_groups.values():
        group.sort(key=lambda item: (item.source_file, item.source_row))
        exact_kept.append(group[0])
        for duplicate in group[1:]:
            summary["exact_duplicates_removed"] += 1
            review_rows.append(
                {
                    "source_file": duplicate.source_file,
                    "source_row": duplicate.source_row,
                    "review_reason": "exact_duplicate_removed",
                    "title": duplicate.title,
                    "location": duplicate.location,
                    "property_type_raw": duplicate.property_type,
                    "price_raw": str(duplicate.price),
                    "bedrooms_raw": "" if duplicate.bedrooms is None else str(duplicate.bedrooms),
                    "bathrooms_raw": "" if duplicate.bathrooms is None else str(duplicate.bathrooms),
                    "added_date_raw": duplicate.added_date,
                    "updated_date_raw": duplicate.updated_date,
                }
            )

    fuzzy_groups: dict[tuple, list[Listing]] = defaultdict(list)
    for row in exact_kept:
        fuzzy_key = (
            row.location_norm,
            row.state,
            row.price,
            row.bedrooms,
            row.bathrooms,
        )
        fuzzy_groups[fuzzy_key].append(row)

    deduped_rows: list[Listing] = []
    for group in fuzzy_groups.values():
        kept: list[Listing] = []
        for candidate in sorted(group, key=lambda item: (item.source_file, item.source_row)):
            matched = False
            for existing in kept:
                title_sim = similarity(candidate.dedupe_title, existing.dedupe_title)
                type_match = candidate.property_type == existing.property_type
                title_contains = (
                    candidate.dedupe_title in existing.dedupe_title
                    or existing.dedupe_title in candidate.dedupe_title
                )
                if title_sim >= 0.82 or (type_match and title_contains):
                    matched = True
                    summary["fuzzy_duplicates_removed"] += 1
                    review_rows.append(
                        {
                            "source_file": candidate.source_file,
                            "source_row": candidate.source_row,
                            "review_reason": "fuzzy_duplicate_removed",
                            "title": candidate.title,
                            "location": candidate.location,
                            "property_type_raw": candidate.property_type,
                            "price_raw": str(candidate.price),
                            "bedrooms_raw": "" if candidate.bedrooms is None else str(candidate.bedrooms),
                            "bathrooms_raw": "" if candidate.bathrooms is None else str(candidate.bathrooms),
                            "added_date_raw": candidate.added_date,
                            "updated_date_raw": candidate.updated_date,
                        }
                    )
                    break
            if not matched:
                kept.append(candidate)
        deduped_rows.extend(kept)

    return deduped_rows, review_rows, summary


def attach_price_category(rows: list[Listing]) -> tuple[list[dict], int]:
    prices = sorted(row.price for row in rows)
    cutoff = int(median(prices))
    final_rows = []
    for row in rows:
        final_rows.append(
            {
                "title": row.title,
                "location": row.location,
                "area_bucket": row.area_bucket,
                "property_type": row.property_type,
                "price": str(row.price),
                "bedrooms": "" if row.bedrooms is None else str(row.bedrooms),
                "bathrooms": "" if row.bathrooms is None else str(row.bathrooms),
                "added_date": row.added_date,
                "updated_date": row.updated_date,
                "month_posted": row.month_posted,
                "state": row.state,
                "price_category": "High" if row.price >= cutoff else "Low",
            }
        )
    return final_rows, cutoff


def build_analysis_ready(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    by_state: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_state[row["state"]].append(row)

    analysis_rows: list[dict] = []
    state_summary: list[dict] = []
    for state, state_rows in sorted(by_state.items()):
        if len(state_rows) < 10:
            continue

        prices = [float(row["price"]) for row in state_rows if float(row["price"]) > 0]
        log_prices = [math.log10(price) for price in prices]
        q1 = quantile(log_prices, 0.25)
        q3 = quantile(log_prices, 0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        kept_rows = []
        for row in state_rows:
            log_price = math.log10(float(row["price"]))
            if lower <= log_price <= upper:
                kept_rows.append(row)

        state_summary.append(
            {
                "state": state,
                "raw_listings": len(state_rows),
                "kept_after_outlier_removal": len(kept_rows),
                "removed_outliers": len(state_rows) - len(kept_rows),
                "average_price": f"{sum(float(r['price']) for r in kept_rows) / len(kept_rows):.2f}",
                "median_price": f"{median(sorted(float(r['price']) for r in kept_rows)):.2f}",
                "min_price": f"{min(float(r['price']) for r in kept_rows):.2f}",
                "max_price": f"{max(float(r['price']) for r in kept_rows):.2f}",
                "lower_log_bound": f"{lower:.4f}",
                "upper_log_bound": f"{upper:.4f}",
            }
        )
        analysis_rows.extend(kept_rows)

    state_summary.sort(key=lambda item: float(item["average_price"]), reverse=True)
    return analysis_rows, state_summary


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    standardized_rows, review_rows, summary = load_and_standardize()
    summary["rows_after_standardization"] = len(standardized_rows)

    deduped_rows, duplicate_review_rows, dedupe_summary = deduplicate(standardized_rows)
    summary.update(dedupe_summary)
    review_rows.extend(duplicate_review_rows)
    summary["rows_after_deduplication"] = len(deduped_rows)

    cleaned_rows, price_category_cutoff = attach_price_category(deduped_rows)
    analysis_ready_rows, state_summary_rows = build_analysis_ready(cleaned_rows)

    summary["price_category_cutoff_median_price"] = price_category_cutoff
    summary["manual_review_rows"] = len(review_rows)
    summary["cleaned_rows_written"] = len(cleaned_rows)
    summary["analysis_ready_rows_written"] = len(analysis_ready_rows)
    summary["states_in_analysis_ready"] = len({row["state"] for row in analysis_ready_rows})
    summary["state_outlier_method"] = "per-state log(price) IQR with 1.5*IQR fences"

    write_csv(CLEANED_OUTPUT, CLEANED_FIELDS, cleaned_rows)
    write_csv(MANUAL_REVIEW_OUTPUT, MANUAL_REVIEW_FIELDS, review_rows)
    write_csv(ANALYSIS_READY_OUTPUT, CLEANED_FIELDS, analysis_ready_rows)
    write_csv(STATE_ANALYSIS_OUTPUT, list(state_summary_rows[0].keys()) if state_summary_rows else [], state_summary_rows)
    SUMMARY_OUTPUT.write_text(json.dumps(summary, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
