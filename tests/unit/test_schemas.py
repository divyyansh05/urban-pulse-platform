"""tests/unit/test_schemas.py — Unit tests for all Pydantic ingestion schemas."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from ingestion.batch.s3_writer import S3Writer
from ingestion.schemas.airnow import AirNowObservationRaw
from ingestion.schemas.base import safe_float, safe_int, safe_str
from ingestion.schemas.nyc_311 import NYC311Raw
from ingestion.schemas.nyc_crime import NYCCrimeRaw
from ingestion.schemas.weather import WeatherObservationRaw


@pytest.fixture
def valid_311():
    return {
        "unique_key": "59812345",
        "created_date": "2024-03-15T14:23:00.000",
        "closed_date": "2024-03-16T09:00:00.000",
        "agency": "NYPD",
        "complaint_type": "Noise - Residential",
        "descriptor": "Loud Music/Party",
        "incident_zip": "10025",
        "incident_address": "123 WEST 86 STREET",
        "borough": "MANHATTAN",
        "latitude": "40.7851",
        "longitude": "-73.9756",
        "status": "Closed",
        "community_board": "07 MANHATTAN",
    }


@pytest.fixture
def valid_crime():
    return {
        "cmplnt_num": "244789123",
        "cmplnt_fr_dt": "03/15/2024",
        "cmplnt_fr_tm": "14:30:00",
        "rpt_dt": "03/15/2024",
        "ofns_desc": "ASSAULT 3",
        "law_cat_cd": "MISDEMEANOR",
        "boro_nm": "MANHATTAN",
        "latitude": "40.7749",
        "longitude": "-73.9851",
        "susp_age_group": "25-44",
        "susp_race": "WHITE HISPANIC",
        "susp_sex": "M",
        "vic_age_group": "25-44",
        "vic_race": "BLACK",
        "vic_sex": "F",
    }


@pytest.fixture
def valid_weather():
    return {
        "time": "2024-03-15T14:00",
        "temperature_2m": 12.4,
        "relative_humidity_2m": 65.0,
        "precipitation": 0.0,
        "precipitation_probability": 10,
        "wind_speed_10m": 18.3,
        "weather_code": 1,
    }


@pytest.fixture
def valid_airnow():
    return {
        "DateObserved": "2024-03-15 ",
        "HourObserved": 14,
        "LocalTimeZone": "EST",
        "ReportingArea": "New York City - Manhattan",
        "StateCode": "NY",
        "Latitude": 40.7128,
        "Longitude": -74.006,
        "ParameterName": "PM2.5",
        "AQI": 42,
        "Category": {"Number": 1, "Name": "Good"},
    }


class TestSafeConversions:
    def test_safe_str_none(self):
        assert safe_str(None) is None

    def test_safe_str_empty(self):
        assert safe_str("") is None

    def test_safe_str_whitespace(self):
        assert safe_str("   ") is None

    def test_safe_str_valid(self):
        assert safe_str("hello") == "hello"

    def test_safe_float_none(self):
        assert safe_float(None) is None

    def test_safe_float_string(self):
        assert safe_float("40.71") == pytest.approx(40.71)

    def test_safe_float_invalid(self):
        assert safe_float("bad") is None

    def test_safe_int_float_string(self):
        assert safe_int("14.0") == 14

    def test_safe_int_invalid(self):
        assert safe_int("abc") is None


class TestNYC311Schema:
    def test_valid_record(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert r.is_valid and r.unique_key == "59812345"

    def test_has_location(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert r.has_location and r.has_borough

    def test_no_location(self, valid_311):
        valid_311.update({"latitude": None, "longitude": None})
        r = NYC311Raw.from_api_record(valid_311)
        assert not r.has_location and r.is_valid

    def test_missing_unique_key_invalid(self, valid_311):
        valid_311["unique_key"] = None
        r = NYC311Raw.from_api_record(valid_311)
        assert not r.is_valid and "missing unique_key" in r.validation_errors

    def test_null_borough_valid(self, valid_311):
        valid_311["borough"] = None
        r = NYC311Raw.from_api_record(valid_311)
        assert r.is_valid and not r.has_borough

    def test_pii_present_in_raw(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert r.incident_address == "123 WEST 86 STREET"

    def test_extra_fields_allowed(self, valid_311):
        valid_311["new_socrata_field"] = "value"
        r = NYC311Raw.from_api_record(valid_311)
        assert r.is_valid

    def test_bq_row_has_required_columns(self, valid_311):
        row = NYC311Raw.from_api_record(valid_311).to_bq_row()
        for col in [
            "_ingestion_id",
            "_ingestion_timestamp",
            "_ingestion_date",
            "unique_key",
            "complaint_type",
            "raw_json",
        ]:
            assert col in row

    def test_raw_json_serialized(self, valid_311):
        r = NYC311Raw.from_api_record(valid_311)
        assert json.loads(r.raw_json)["unique_key"] == "59812345"


class TestNYCCrimeSchema:
    def test_valid_record(self, valid_crime):
        r = NYCCrimeRaw.from_api_record(valid_crime)
        assert r.is_valid and r.cmplnt_num == "244789123"

    def test_missing_cmplnt_num_invalid(self, valid_crime):
        valid_crime["cmplnt_num"] = None
        assert not NYCCrimeRaw.from_api_record(valid_crime).is_valid

    def test_date_time_separate(self, valid_crime):
        r = NYCCrimeRaw.from_api_record(valid_crime)
        assert r.cmplnt_fr_dt == "03/15/2024" and r.cmplnt_fr_tm == "14:30:00"

    def test_pii_adjacent_present(self, valid_crime):
        r = NYCCrimeRaw.from_api_record(valid_crime)
        assert r.susp_race == "WHITE HISPANIC" and r.vic_age_group == "25-44"


class TestWeatherSchema:
    def test_valid_nyc(self, valid_weather):
        r = WeatherObservationRaw.from_unpacked_row(valid_weather, city="nyc")
        assert r.is_valid and r.city == "nyc" and r.temperature_2m == pytest.approx(12.4)

    def test_london_coords(self, valid_weather):
        r = WeatherObservationRaw.from_unpacked_row(valid_weather, city="london")
        assert r.latitude == pytest.approx(51.5074)

    def test_unknown_city_invalid(self, valid_weather):
        assert not WeatherObservationRaw.from_unpacked_row(valid_weather, city="paris").is_valid

    def test_null_values_ok(self, valid_weather):
        valid_weather.update({"precipitation": None, "wind_speed_10m": None})
        r = WeatherObservationRaw.from_unpacked_row(valid_weather, city="nyc")
        assert r.is_valid and r.precipitation is None


class TestAirNowSchema:
    def test_valid_record(self, valid_airnow):
        r = AirNowObservationRaw.from_api_record(valid_airnow)
        assert r.is_valid and r.parameter_name == "PM2.5" and r.aqi == 42

    def test_nested_category_parsed(self, valid_airnow):
        r = AirNowObservationRaw.from_api_record(valid_airnow)
        assert r.category_number == 1 and r.category_name == "Good"

    def test_missing_parameter_invalid(self, valid_airnow):
        valid_airnow["ParameterName"] = None
        assert not AirNowObservationRaw.from_api_record(valid_airnow).is_valid

    def test_aqi_out_of_range(self, valid_airnow):
        valid_airnow["AQI"] = 999
        r = AirNowObservationRaw.from_api_record(valid_airnow)
        assert any("aqi out of range" in e for e in r.validation_errors)


class TestS3Writer:
    def test_key_format(self):
        w = S3Writer.__new__(S3Writer)
        w.bucket = "test"
        dt = datetime(2024, 3, 15, tzinfo=UTC)
        assert w.build_s3_key("nyc/311", "abc", dt) == "nyc/311/2024/03/15/abc.ndjson"

    def test_zero_padded_month(self):
        w = S3Writer.__new__(S3Writer)
        w.bucket = "test"
        dt = datetime(2024, 1, 5, tzinfo=UTC)
        assert "2024/01/05" in w.build_s3_key("nyc/crime", "r1", dt)

    def test_empty_records_no_upload(self):
        w = S3Writer.__new__(S3Writer)
        w.bucket = "test"
        w._client = MagicMock()
        assert w.write_records([], "nyc/311", "run-1") == ""
        w._client.upload_fileobj.assert_not_called()
