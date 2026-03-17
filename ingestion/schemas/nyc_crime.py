"""ingestion/schemas/nyc_crime.py — Pydantic model for NYPD Complaint Data."""

from __future__ import annotations

import json

from pydantic import BaseModel, Field, model_validator

from ingestion.schemas.base import new_uuid, safe_str, today_utc, utc_now

VALID_LAW_CATS = {"FELONY", "MISDEMEANOR", "VIOLATION"}


class NYCCrimeRaw(BaseModel):
    ingestion_id: str = Field(default_factory=new_uuid)
    ingestion_timestamp: str = Field(default_factory=lambda: utc_now().isoformat())
    ingestion_date: str = Field(default_factory=lambda: str(today_utc()))
    source_file: str | None = None
    raw_json: str = ""

    cmplnt_num: str | None = None
    cmplnt_fr_dt: str | None = None
    cmplnt_fr_tm: str | None = None
    cmplnt_to_dt: str | None = None
    cmplnt_to_tm: str | None = None
    rpt_dt: str | None = None
    ofns_desc: str | None = None
    pd_desc: str | None = None
    law_cat_cd: str | None = None
    boro_nm: str | None = None
    addr_pct_cd: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    susp_age_group: str | None = None
    susp_race: str | None = None
    susp_sex: str | None = None
    vic_age_group: str | None = None
    vic_race: str | None = None
    vic_sex: str | None = None

    is_valid: bool = True
    validation_errors: list[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def validate_record(self) -> NYCCrimeRaw:
        errors = []
        if not self.cmplnt_num:
            errors.append("missing cmplnt_num")
        if not self.cmplnt_fr_dt:
            errors.append("missing cmplnt_fr_dt")
        if self.law_cat_cd and self.law_cat_cd.upper() not in VALID_LAW_CATS:
            errors.append(f"unknown law_cat_cd: {self.law_cat_cd}")
        if errors:
            self.validation_errors = errors
            if "missing cmplnt_num" in errors:
                self.is_valid = False
        return self

    @classmethod
    def from_api_record(cls, record: dict, source_file: str | None = None) -> NYCCrimeRaw:
        return cls(
            raw_json=json.dumps(record, default=str),
            source_file=source_file,
            cmplnt_num=safe_str(record.get("cmplnt_num")),
            cmplnt_fr_dt=safe_str(record.get("cmplnt_fr_dt")),
            cmplnt_fr_tm=safe_str(record.get("cmplnt_fr_tm")),
            cmplnt_to_dt=safe_str(record.get("cmplnt_to_dt")),
            cmplnt_to_tm=safe_str(record.get("cmplnt_to_tm")),
            rpt_dt=safe_str(record.get("rpt_dt")),
            ofns_desc=safe_str(record.get("ofns_desc")),
            pd_desc=safe_str(record.get("pd_desc")),
            law_cat_cd=safe_str(record.get("law_cat_cd")),
            boro_nm=safe_str(record.get("boro_nm")),
            addr_pct_cd=safe_str(record.get("addr_pct_cd")),
            latitude=safe_str(record.get("latitude")),
            longitude=safe_str(record.get("longitude")),
            susp_age_group=safe_str(record.get("susp_age_group")),
            susp_race=safe_str(record.get("susp_race")),
            susp_sex=safe_str(record.get("susp_sex")),
            vic_age_group=safe_str(record.get("vic_age_group")),
            vic_race=safe_str(record.get("vic_race")),
            vic_sex=safe_str(record.get("vic_sex")),
        )

    def to_bq_row(self) -> dict:
        return {
            "_ingestion_id": self.ingestion_id,
            "_ingestion_timestamp": self.ingestion_timestamp,
            "_ingestion_date": self.ingestion_date,
            "_source_file": self.source_file,
            "cmplnt_num": self.cmplnt_num,
            "cmplnt_fr_dt": self.cmplnt_fr_dt,
            "cmplnt_fr_tm": self.cmplnt_fr_tm,
            "cmplnt_to_dt": self.cmplnt_to_dt,
            "cmplnt_to_tm": self.cmplnt_to_tm,
            "rpt_dt": self.rpt_dt,
            "ofns_desc": self.ofns_desc,
            "pd_desc": self.pd_desc,
            "law_cat_cd": self.law_cat_cd,
            "boro_nm": self.boro_nm,
            "addr_pct_cd": self.addr_pct_cd,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "susp_age_group": self.susp_age_group,
            "susp_race": self.susp_race,
            "susp_sex": self.susp_sex,
            "vic_age_group": self.vic_age_group,
            "vic_race": self.vic_race,
            "vic_sex": self.vic_sex,
            "raw_json": self.raw_json,
        }
