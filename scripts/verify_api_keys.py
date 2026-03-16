import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env"

# Always load from repository .env so running from any cwd works.
load_dotenv(dotenv_path=ENV_PATH)


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


def getenv_stripped(name: str) -> str:
    value = os.getenv(name, "")
    return value.strip()


def report_http(name: str, response: requests.Response, expected: int = 200) -> bool:
    ok = response.status_code == expected
    status = "OK" if ok else "FAIL"
    print(f"{status} {name}: HTTP {response.status_code}")
    return ok


def missing_var(name: str) -> bool:
    value = getenv_stripped(name)
    if not value:
        print(f"SKIP {name}: missing in .env")
        return True
    return False


def check_nyc() -> bool:
    if missing_var("NYC_APP_TOKEN"):
        return False
    url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    headers = {"X-App-Token": getenv_stripped("NYC_APP_TOKEN")}
    params = {"$limit": 1}
    try:
        r = SESSION.get(url, headers=headers, params=params, timeout=30)
        return report_http("NYC Open Data", r)
    except Exception as exc:
        print(f"FAIL NYC Open Data: {exc}")
        return False


def check_tfl() -> bool:
    if missing_var("TFL_APP_KEY"):
        return False
    url = "https://api.tfl.gov.uk/Line/victoria/Status"
    params = {"app_key": getenv_stripped("TFL_APP_KEY")}
    try:
        r = SESSION.get(url, params=params, timeout=20)
        return report_http("TfL", r)
    except Exception as exc:
        print(f"FAIL TfL: {exc}")
        return False


def check_airnow() -> bool:
    if missing_var("AIRNOW_API_KEY"):
        return False
    url = "https://www.airnowapi.org/aq/observation/latLong/current/"
    params = {
        "format": "application/json",
        "latitude": "40.71",
        "longitude": "-74.00",
        "distance": "25",
        "API_KEY": getenv_stripped("AIRNOW_API_KEY"),
    }
    try:
        r = SESSION.get(url, params=params, timeout=20)
        return report_http("AirNow EPA", r)
    except Exception as exc:
        print(f"FAIL AirNow EPA: {exc}")
        return False


def check_open_meteo() -> bool:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": "40.71",
        "longitude": "-74.00",
        "current_weather": "true",
    }
    try:
        r = SESSION.get(url, params=params, timeout=20)
        return report_http("Open-Meteo (no key needed)", r)
    except Exception as exc:
        print(f"FAIL Open-Meteo: {exc}")
        return False


def check_noaa() -> bool:
    if missing_var("NOAA_CDO_TOKEN"):
        return False
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets"
    headers = {"token": getenv_stripped("NOAA_CDO_TOKEN")}
    params = {"limit": 1}
    try:
        r = SESSION.get(url, headers=headers, params=params, timeout=20)
        return report_http("NOAA CDO", r)
    except Exception as exc:
        print(f"FAIL NOAA CDO: {exc}")
        return False


def main() -> int:
    print(f"Using env file: {ENV_PATH}")
    results = [
        check_nyc(),
        check_tfl(),
        check_airnow(),
        check_open_meteo(),
        check_noaa(),
    ]
    failed = len([x for x in results if not x])
    print(f"Summary: {len(results) - failed}/{len(results)} checks passed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
