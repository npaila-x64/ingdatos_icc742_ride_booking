# Docker Setup Guide

This guide explains how to run the Ride Booking ETL project using Docker and Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 2GB of free disk space

## Quick Start

### 1. Configure Environment

Copy the Docker environment template:

```bash
cp .env.docker .env
```

Edit `.env` with your desired credentials and settings.

### 2. Build and Start Services

#### Minimal Setup (PostgreSQL + ETL App)
```bash
make up
# or
docker-compose up -d postgres etl_app
```

#### Development Setup (with pgAdmin)
```bash
make up-dev
# or
docker-compose --profile dev up -d
```

#### Full Setup (with Prefect Server)
```bash
make up-prefect
# or
docker-compose --profile prefect up -d
```

### 3. Verify Services

Check that services are running:
```bash
make status
# or
docker-compose ps
```

## Services

### PostgreSQL Database
- **Port**: 5432 (configurable via `DB_PORT`)
- **User**: postgres (configurable via `DB_USER`)
- **Database**: ride_booking (configurable via `DB_NAME`)
- **Connection**: `postgresql://postgres:postgres_secret_password@localhost:5432/ride_booking`

### pgAdmin (Development Only)
- **URL**: http://localhost:5050
- **Email**: admin@ridebooking.com (configurable via `PGADMIN_EMAIL`)
- **Password**: admin_password (configurable via `PGADMIN_PASSWORD`)

To add PostgreSQL server in pgAdmin:
1. Open http://localhost:5050
2. Login with credentials
3. Add New Server:
   - Name: Ride Booking DB
   - Host: postgres
   - Port: 5432
   - Username: postgres
   - Password: (your DB_PASSWORD)

### Prefect Server (Optional)
- **URL**: http://localhost:4200
- **API**: http://localhost:4200/api

### ETL Application
- Connects to PostgreSQL automatically
- Mounts local `data/` and `app/` directories
- Hot-reload enabled for development

## Common Commands

### Using Make (Recommended)

```bash
# View all available commands
make help

# Build images
make build

# Start services
make up              # Minimal
make up-dev          # With pgAdmin
make up-prefect      # With Prefect

# View logs
make logs            # All services
make logs-etl        # ETL app only
make logs-db         # PostgreSQL only

# Access shells
make db-shell        # PostgreSQL psql
make etl-shell       # Bash in ETL container
make etl-python      # Python REPL in ETL container

# Run application
make bootstrap       # Run bootstrap script
make example         # Run example usage

# Stop and clean
make down            # Stop services
make clean           # Remove everything
```

### Using Docker Compose Directly

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Rebuild and restart
docker-compose up -d --build

# Execute commands in containers
docker-compose exec etl_app python app/adapters/example_usage.py
docker-compose exec postgres psql -U postgres -d ride_booking
```

## Data Persistence

Data is persisted using Docker volumes:

- `postgres_data`: PostgreSQL database files
- `pgadmin_data`: pgAdmin configuration
- `etl_logs`: ETL application logs
- `prefect_data`: Prefect server data

To completely reset the database:
```bash
make clean
# or
docker-compose down -v
```

## Development Workflow

### 1. Code Changes

The `app/` directory is mounted as a volume, so code changes are reflected immediately:

```bash
# Edit files locally
vim app/adapters/postgresql.py

# Changes are available in container immediately
make etl-python
>>> from app.adapters import PostgreSQLAdapter
```

### 2. Run Tests

```bash
make test
# or
docker-compose exec etl_app pytest -v
```

### 3. Interactive Development

```bash
# Open Python REPL in container
make etl-python

# Or bash shell
make etl-shell
```

### 4. Database Operations

```bash
# Connect to database
make db-shell

# Run SQL
psql> \dt              # List tables
psql> \dn              # List schemas
psql> SELECT * FROM rides LIMIT 10;
```

## Profiles

Docker Compose profiles allow you to selectively start services:

- **Default**: `postgres`, `etl_app`
- **dev**: Adds `pgadmin`
- **prefect**: Adds `prefect_server`

## Networking

All services run on the `ride_booking_network` bridge network:

- Services can communicate using service names
- ETL app connects to PostgreSQL at `postgres:5432`
- Prefect server accessible at `prefect_server:4200`

## Environment Variables

Key environment variables (set in `.env`):

```bash
# Database
DB_USER=postgres
DB_PASSWORD=postgres_secret_password
DB_NAME=ride_booking
DB_PORT=5432
DB_SCHEMA=public

# pgAdmin
PGADMIN_EMAIL=admin@ridebooking.com
PGADMIN_PASSWORD=admin_password
PGADMIN_PORT=5050

# Prefect
PREFECT_PORT=4200
PREFECT_PROFILE=ride-booking-docker
PREFECT_API_URL=http://prefect_server:4200/api
```

## Troubleshooting

### Services won't start
```bash
# Check logs
make logs

# Check service status
make status

# Rebuild images
make build
make up
```

### Database connection errors
```bash
# Verify PostgreSQL is healthy
docker-compose exec postgres pg_isready -U postgres

# Check connection from ETL app
docker-compose exec etl_app python -c "from app.adapters import PostgreSQLAdapter; from app.config.settings import load_settings; from dotenv import load_dotenv; load_dotenv(); s = load_settings(); a = PostgreSQLAdapter(s.database); print(a.execute_query('SELECT 1'))"
```

### Port conflicts
If ports are already in use, change them in `.env`:
```bash
DB_PORT=5433        # Instead of 5432
PGADMIN_PORT=5051   # Instead of 5050
PREFECT_PORT=4201   # Instead of 4200
```

### Permission errors
```bash
# Fix ownership of data directory
sudo chown -R $USER:$USER data/
chmod -R 755 data/
```

## Production Considerations

For production deployments:

1. **Use secrets management** instead of `.env` files
2. **Enable SSL/TLS** for PostgreSQL connections
3. **Configure resource limits** in docker-compose.yml
4. **Use external volumes** for backups
5. **Set up monitoring** and alerting
6. **Use a reverse proxy** (nginx/traefik) for services
7. **Enable authentication** for all services
8. **Regular backups** of PostgreSQL data

Example production adjustments:

```yaml
# In docker-compose.yml
services:
  etl_app:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
    restart: always
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

## Additional Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Docker Image](https://hub.docker.com/_/postgres)
- [Prefect Documentation](https://docs.prefect.io/)
