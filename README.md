# Ride Booking ETL with Prefect

Prefect-based ETL scaffolding for the ingdatos ICC742 ride booking initiative. The
repository contains the project skeleton, environment bootstrap helpers, and
configuration utilities required before building Prefect flows.

## Requirements
- Python 3.10+
- Prefect Cloud account or Prefect Server (optional for local development)
- Access to the ride booking data sources (to be integrated in future tasks)

## Quickstart
1. Create and activate a Python virtual environment.
2. Install dependencies in editable mode:
	 ```bash
	 pip install -e .[dev]
	 ```
3. Copy `.env.example` to `.env` and adjust values to match your environment.
4. Run the bootstrap script to create directories, persist a settings snapshot, and
	 register the Prefect profile:
	 ```bash
	 ride-booking-bootstrap
	 ```

## Prefect Setup Notes
- The default Prefect profile is `ride-booking-local`; adjust `PREFECT_PROFILE` in
	your `.env` file if you need a different workspace.
- The bootstrap helper creates `data/raw`, `data/processed`, and `data/logs` to
	isolate extracted assets from derived datasets.
- Update `PREFECT_API_URL`, `PREFECT_STORAGE_BLOCK`, and `PREFECT_WORK_POOL` in `.env`
	once infrastructure decisions are finalized.

## Next Steps
- Define Prefect blocks for storage, messaging, and credentials.
- Model ETL flows inside `app/etl` using the project settings helpers.
- Add automated tests around reusable components as flows are introduced.