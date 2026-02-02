# uvceed_alerts/geo.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
from pathlib import Path
import re

import requests

ZIPPOTAM_URL_TMPL = "https://api.zippopotam.us/us/{zip}"
FCC_BLOCK_URL = "https://geo.fcc.gov/api/census/block/find"

UA_API = "uvceed-alerts-geo/0.1 (+uvceed)"
DEFAULT_TIMEOUT = 20


@dataclass(frozen=True)
class GeoResult:
    zip_code: str
    place: str
    state_abbr: str
    state_name: str
    latitude: float
    longitude: float
    county_name: str
    county_fips: str  # 5-digit county FIPS (state+county)


class GeoError(RuntimeError):
    """Raised when a ZIP cannot be resolved to a county FIPS."""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA_API, "Accept": "application/json"})
    return s


def zip_to_place_latlon(zip_code: str, *, timeout: int = DEFAULT_TIMEOUT) -> Tuple[str, str, str, float, float]:
    """
    ZIP -> (place, state_abbr, state_name, lat, lon) via Zippopotam.us
    """
    if not re.fullmatch(r"\d{5}", zip_code.strip()):
        raise GeoError(f"Invalid ZIP code: {zip_code!r} (expected 5 digits)")

    s = _session()
    url = ZIPPOTAM_URL_TMPL.format(zip=zip_code.strip())
    r = s.get(url, timeout=timeout)
    if r.status_code == 404:
        raise GeoError(f"ZIP not found: {zip_code}")
    r.raise_for_status()

    data = r.json()
    places = data.get("places") or []
    if not places:
        raise GeoError(f"ZIP {zip_code} returned no places.")

    p0 = places[0]
    place = (p0.get("place name") or "").strip()
    state_abbr = (p0.get("state abbreviation") or "").strip()
    state_name = (p0.get("state") or "").strip()

    try:
        lat = float(p0["latitude"])
        lon = float(p0["longitude"])
    except Exception as e:
        raise GeoError(f"ZIP {zip_code} did not provide valid lat/lon: {e}")

    return place, state_abbr, state_name, lat, lon


def latlon_to_county(lat: float, lon: float, *, timeout: int = DEFAULT_TIMEOUT) -> Tuple[str, str]:
    """
    lat/lon -> (county_name, county_fips) via FCC Census Block API.
    Returns county_fips as a 5-character string (state+county FIPS).
    """
    s = _session()
    params = {
        "format": "json",
        "latitude": lat,
        "longitude": lon,
        "showall": "false",
    }
    r = s.get(FCC_BLOCK_URL, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    county = data.get("County") or {}
    county_name = (county.get("name") or "").strip()
    county_fips = (county.get("FIPS") or "").strip()

    if not county_name or not county_fips or not re.fullmatch(r"\d{5}", county_fips):
        raise GeoError(f"FCC lookup did not return valid county info for lat/lon={lat},{lon}")

    return county_name, county_fips


def zip_to_county(zip_code: str, *, timeout: int = DEFAULT_TIMEOUT) -> GeoResult:
    """
    Convenience: ZIP -> GeoResult (place/state + county FIPS).
    """
    place, state_abbr, state_name, lat, lon = zip_to_place_latlon(zip_code, timeout=timeout)
    county_name, county_fips = latlon_to_county(lat, lon, timeout=timeout)

    return GeoResult(
        zip_code=zip_code.strip(),
        place=place,
        state_abbr=state_abbr,
        state_name=state_name,
        latitude=lat,
        longitude=lon,
        county_name=county_name,
        county_fips=county_fips,
    )


def _format(res: GeoResult) -> str:
    return (
        f"ZIP: {res.zip_code}\n"
        f"Place: {res.place}\n"
        f"State: {res.state_name} ({res.state_abbr})\n"
        f"Lat/Lon: {res.latitude:.6f}, {res.longitude:.6f}\n"
        f"County: {res.county_name}\n"
        f"County FIPS: {res.county_fips}\n"
    )


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Resolve a US ZIP code to county FIPS (ZIP -> lat/lon -> county).")
    parser.add_argument("zip", help="5-digit ZIP code (e.g., 60614)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds (default 20)")
    args = parser.parse_args(argv)

    try:
        res = zip_to_county(args.zip, timeout=args.timeout)
        print(_format(res))
        return 0
    except GeoError as e:
        print(f"[geo] {e}")
        return 2
    except requests.RequestException as e:
        print(f"[geo] network error: {e}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

