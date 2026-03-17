# API Exploration Summary
Generated: 2026-03-16T23:59:20.213269+00:00

## Status

| Source | Status | Volume | Key Notes |
|--------|--------|--------|-----------|
| NYC 311 | ✅ OK | 20510665 | SCD Type 2 needed for status tracking |
| NYC Crime | ✅ OK | 9491946 | PII-adjacent fields need governance tagging |
| TfL Transit | ✅ OK | streaming | Streaming — 30s poll cycle via Pub/Sub |
| AirNow EPA | ✅ OK | hourly observations | Pivot pollutant rows in staging layer |
| Open-Meteo Weather | ✅ OK | hourly | Columnar response — must unpack arrays into rows |

## Key Engineering Findings

### Ingestion Patterns Required
| Pattern | Sources |
|---------|---------|
| Streaming (30s poll) | TfL Transit |
| Hourly batch | AirNow EPA, Open-Meteo Weather |
| Daily batch | NYC Crime (NYPD) |
| Near real-time (event-driven) | NYC 311 |

### Data Quality Challenges Identified
1. **NYC 311** — `borough` field is null for ~15% of records. Need zip→borough lookup table.
2. **NYC Crime** — Date and time in separate fields. Some records have null coordinates.
3. **TfL** — `statusSeverity` is numeric (not descriptive). Need severity lookup seed table.
4. **AirNow** — Multiple rows per timestamp (one per pollutant). Needs pivoting in staging.
5. **Open-Meteo** — Columnar array format. Must unpack before loading.

### PII / Governance Flags
- NYC Crime: `susp_race`, `susp_age_group`, `vic_race`, `vic_age_group` — tag as sensitive
- NYC 311: `incident_address` — tag as PII, pseudonymise to zip+borough only in marts

### Schema Evolution Risks
- NYC 311 API has changed field names twice in past 3 years (Socrata versioned)
- TfL occasionally adds new `lineStatuses` array fields during disruptions
- AirNow adds new pollutant parameters (e.g. AQI_CO added 2023)

### Seed Tables Needed (Phase 1)
1. `borough_zip_lookup` — maps NYC zip codes to boroughs
2. `tfl_severity_lookup` — maps TfL statusSeverity integer to description
3. `wmo_weather_code_lookup` — maps Open-Meteo WMO codes to descriptions
4. `nyc_community_board_lookup` — maps community board numbers to neighborhoods

## Files Generated
- `docs/architecture/api_schemas/airnow_schema.json`
- `docs/architecture/api_schemas/nyc_311_schema.json`
- `docs/architecture/api_schemas/nyc_crime_schema.json`
- `docs/architecture/api_schemas/tfl_transit_schema.json`
- `docs/architecture/api_schemas/weather_schema.json`
