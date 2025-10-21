-- Initial database setup script
-- This script runs automatically when PostgreSQL container is first created

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ride_analytics;

-- Set default schema
ALTER DATABASE ride_booking SET search_path TO public, bronze, silver, gold, ride_analytics;

-- Create a sample table structure (optional)
CREATE TABLE IF NOT EXISTS public.rides (
    id SERIAL PRIMARY KEY,
    ride_id VARCHAR(50) UNIQUE NOT NULL,
    passenger_id VARCHAR(50),
    driver_id VARCHAR(50),
    pickup_location VARCHAR(255),
    dropoff_location VARCHAR(255),
    distance_km DECIMAL(10, 2),
    duration_minutes INTEGER,
    fare DECIMAL(10, 2),
    tip DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    ride_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_rides_passenger_id ON public.rides(passenger_id);
CREATE INDEX IF NOT EXISTS idx_rides_driver_id ON public.rides(driver_id);
CREATE INDEX IF NOT EXISTS idx_rides_created_at ON public.rides(created_at);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ride_analytics TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ride_analytics TO postgres;
