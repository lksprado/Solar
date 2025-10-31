# Home Solar: From Scrape to Insights

Build a clean, automated pipeline that extracts and transforms production data from a home solar system with no public API. This repository focuses on robust extraction and transformation — designed to plug into a personal Airflow repository as a submodule. The data load/orchestration stage lives outside this repo by design.

Note on scope: This project is a submodule of a personal Airflow setup. The load stage is intentionally not included here.

## Why This Exists
- No official API: Data is hidden behind a login and a specific app view that enables an internal API.
- Practical engineering: Demonstrates resilient scraping, structured transformations, and testable Python without over-engineering.
- Personal analytics: Feeds a simple, consistent dataset for downstream visualization.

## What It Does
- Logs into the solar provider’s portal and fetches historical and current production data.
- Transforms raw JSON into tidy, analysis-ready DataFrames (hourly and daily summaries).
- Writes control artifacts (e.g., missing date lists) to ensure continuity across runs.

## What It Doesn’t Do (Here)
- Orchestration and loading to databases/data lake. These are handled by the parent Airflow repository where this module is consumed.

## Tech Stack
- Selenium: Reliable browser automation to reach the internal API endpoints.
- Python + OOP: Clear separation of concerns and maintainability.
- Pytest: Function-level tests for critical components.
- Logging: Structured logs to aid debugging and observability.

## Key Modules
- `src/missing_raw.py`: Identifies dates with missing data and writes them to a control file.
- `src/extraction.py`: Authenticates and pulls raw JSON from the portal (via Selenium-enabled flows).
- `src/transforming.py`: Parses JSON to pandas DataFrames and produces hourly and daily aggregates.
- `main.py`: Example runner wiring the steps together for local/debug usage.

Associated tests are in `tests/` for extraction, transformation, and (where applicable) database-related helpers.

## Typical Flow
1) Identify gaps: Generate/update a list of missing dates.
2) Extract data: Log in, navigate to the correct view, and request per-date JSON.
3) Transform data: Normalize, clean, and aggregate into hourly and daily tables.

Downstream loading/orchestration is performed by Airflow in the parent repository.

## Quick Start (Local)
- Set credentials in `.env` (`USERNAME`, `PASSWORD`).
- Run `python main.py` to execute the identify → extract → transform sequence.
- Staging/output locations are configured inside `main.py` and via environment variables.

## Visualization
Public dashboard (sample): https://public.tableau.com/app/profile/lucas8230/viz/HOMESOLARPANELPRODUCTION2021-2024/Painel1

## Notes
- This is a personal, non-replicable setup tailored to a specific provider.
- Network behavior and UI flows may change; scraping logic is built to be adaptable but may require updates over time.
