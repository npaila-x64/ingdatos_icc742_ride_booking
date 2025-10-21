# Ride Booking ETL with Prefect

Prefect-based ETL scaffolding for the ingdatos ICC742 ride booking initiative. The
repository contains the project skeleton, environment bootstrap helpers, and
configuration utilities required before building Prefect flows.

## Features
- **PostgreSQL Integration**: Fully-featured database adapter with SQLAlchemy
- **Prefect Orchestration**: ETL workflow management and scheduling
- **Configuration Management**: Environment-based settings with strong typing
- **Data Processing**: pandas integration for data transformations
- **Docker Support**: Containerized deployment with Docker Compose

## Requirements

### Local Development
- Python 3.10+
- PostgreSQL 12+ (for database operations)
- Prefect Cloud account or Prefect Server (optional for local development)
- Access to the ride booking data sources (to be integrated in future tasks)

### Docker Deployment
- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 2GB of free disk space

## Installation Options

### Option 1: Docker (Recommended)

See [DOCKER.md](DOCKER.md) for detailed Docker setup instructions.

**Quick Start:**
```bash
# Copy environment template
cp .env.docker .env

# Start services (PostgreSQL + ETL App)
make up
# or
docker-compose up -d

# Access services:
# - PostgreSQL: localhost:5432
# - pgAdmin (dev): http://localhost:5050
# - Prefect (optional): http://localhost:4200
```

**Common Docker Commands:**
```bash
make help          # View all available commands
make up-dev        # Start with pgAdmin
make logs          # View logs
make db-shell      # Connect to PostgreSQL
make etl-shell     # Open ETL container shell
make example       # Run example usage
make down          # Stop services
```

### Option 2: Local Installation
### Option 2: Local Installation

1. Create and activate a Python virtual environment.
2. Install dependencies in editable mode:
	 ```bash
	 pip install -e .[dev]
	 ```
3. Copy `.env.example` to `.env` and adjust values to match your environment:
	 ```bash
	 cp .env.example .env
	 # Edit .env with your PostgreSQL credentials
	 ```
4. Run the bootstrap script to create directories, persist a settings snapshot, and
	 register the Prefect profile:
	 ```bash
	 ride-booking-bootstrap
	 ```

## PostgreSQL Adapter
The project includes a comprehensive PostgreSQL adapter for database operations:

### Basic Usage
```python
from app.adapters.postgresql import PostgreSQLAdapter
from app.config.settings import load_settings
from dotenv import load_dotenv

# Load configuration
load_dotenv()
settings = load_settings()

# Initialize adapter
adapter = PostgreSQLAdapter(settings.database)

# Read data
df = adapter.read_table("rides", columns=["id", "passenger_id", "fare"])

# Write data
adapter.write_table(df, "processed_rides", if_exists="append")

# Execute queries
results = adapter.execute_query(
    "SELECT * FROM rides WHERE fare > :min_fare",
    params={"min_fare": 20.0}
)

# Clean up
adapter.close()
```

### Context Manager Pattern
```python
with PostgreSQLAdapter(settings.database) as adapter:
    df = adapter.read_table("rides")
    # Process data...
    adapter.write_table(df, "processed_rides")
```

### Available Methods
- `execute_query()`: Run SELECT queries and get results as dictionaries
- `execute_statement()`: Run INSERT/UPDATE/DELETE statements
- `read_table()`: Load database tables into pandas DataFrames
- `write_table()`: Write pandas DataFrames to database tables
- `table_exists()`: Check if a table exists
- `create_schema()`: Create database schemas

See `app/adapters/example_usage.py` for comprehensive examples.

## Configuration
The project uses environment variables for configuration. Key settings:

### Database Settings
- `DB_HOST`: PostgreSQL host (default: localhost)
- `DB_PORT`: PostgreSQL port (default: 5432)
- `DB_NAME`: Database name (default: ride_booking)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password
- `DB_SCHEMA`: Default schema (default: public)

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

## Project Structure
```
.
├── app/
│   ├── adapters/          # Database and external service adapters
│   │   ├── postgresql.py  # PostgreSQL adapter
│   │   └── example_usage.py
│   ├── config/            # Configuration management
│   │   └── settings.py
│   └── etl/               # ETL workflows and utilities
│       └── bootstrap.py
├── data/                  # Data files (bronze, silver, gold)
├── init-db/              # Database initialization scripts
├── Dockerfile            # Docker container definition
├── docker-compose.yml    # Multi-container orchestration
├── Makefile              # Convenience commands
├── DOCKER.md             # Docker setup guide
└── README.md             # This file
```

## Documentation
- [Docker Setup Guide](DOCKER.md) - Comprehensive Docker deployment instructions
- [Example Usage](app/adapters/example_usage.py) - PostgreSQL adapter examples