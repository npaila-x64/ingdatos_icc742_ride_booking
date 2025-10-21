-- Medallion Architecture Schema for Ride Booking ETL
-- Bronze -> Silver -> Gold layers

-- ============================================================================
-- BRONZE LAYER: Raw staging tables partitioned by month
-- ============================================================================

-- Bronze: Customer (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.customer (
    customer_id VARCHAR(50) NOT NULL,
    booking_id VARCHAR(50) NOT NULL,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL, -- YYYY-MM format
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id, booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_customer_month ON bronze.customer(extraction_month);
CREATE INDEX IF NOT EXISTS idx_bronze_customer_date ON bronze.customer(extraction_date);

-- Bronze: Vehicle Type (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.vehicle_type (
    vehicle_type_name VARCHAR(100) NOT NULL,
    booking_id VARCHAR(50) NOT NULL,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (vehicle_type_name, booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_vehicle_type_month ON bronze.vehicle_type(extraction_month);

-- Bronze: Location (raw extraction for both pickup and drop)
CREATE TABLE IF NOT EXISTS bronze.location (
    location_name VARCHAR(255) NOT NULL,
    location_type VARCHAR(20) NOT NULL, -- 'pickup' or 'drop'
    booking_id VARCHAR(50) NOT NULL,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (location_name, location_type, booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_location_month ON bronze.location(extraction_month);
CREATE INDEX IF NOT EXISTS idx_bronze_location_type ON bronze.location(location_type);

-- Bronze: Booking (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.booking (
    booking_id VARCHAR(50) NOT NULL,
    booking_date DATE NOT NULL,
    booking_time TIME NOT NULL,
    booking_status VARCHAR(50),
    customer_id VARCHAR(50),
    vehicle_type VARCHAR(100),
    pickup_location VARCHAR(255),
    drop_location VARCHAR(255),
    booking_value DECIMAL(10, 2),
    payment_method VARCHAR(50),
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_booking_month ON bronze.booking(extraction_month);
CREATE INDEX IF NOT EXISTS idx_bronze_booking_date ON bronze.booking(booking_date);
CREATE INDEX IF NOT EXISTS idx_bronze_booking_status ON bronze.booking(booking_status);

-- Bronze: Booking Status (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.booking_status (
    booking_status_name VARCHAR(50) NOT NULL,
    booking_id VARCHAR(50) NOT NULL,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (booking_status_name, booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_booking_status_month ON bronze.booking_status(extraction_month);

-- Bronze: Payment Method (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.payment_method (
    payment_method_name VARCHAR(50) NOT NULL,
    booking_id VARCHAR(50) NOT NULL,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (payment_method_name, booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_payment_method_month ON bronze.payment_method(extraction_month);

-- Bronze: Ride (raw extraction for completed rides)
CREATE TABLE IF NOT EXISTS bronze.ride (
    booking_id VARCHAR(50) NOT NULL,
    avg_vtat DECIMAL(10, 2),
    avg_ctat DECIMAL(10, 2),
    ride_distance DECIMAL(10, 2),
    driver_rating DECIMAL(3, 1),
    customer_rating DECIMAL(3, 1),
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_ride_month ON bronze.ride(extraction_month);

-- Bronze: Cancelled Ride (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.cancelled_ride (
    booking_id VARCHAR(50) NOT NULL,
    cancelled_by VARCHAR(20), -- 'Customer' or 'Driver'
    cancellation_reason TEXT,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (booking_id, cancelled_by, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_cancelled_ride_month ON bronze.cancelled_ride(extraction_month);
CREATE INDEX IF NOT EXISTS idx_bronze_cancelled_ride_by ON bronze.cancelled_ride(cancelled_by);

-- Bronze: Incompleted Ride (raw extraction)
CREATE TABLE IF NOT EXISTS bronze.incompleted_ride (
    booking_id VARCHAR(50) NOT NULL,
    incompletion_reason TEXT,
    extraction_date DATE NOT NULL,
    extraction_month VARCHAR(7) NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (booking_id, extraction_month)
);

CREATE INDEX IF NOT EXISTS idx_bronze_incompleted_ride_month ON bronze.incompleted_ride(extraction_month);

-- ============================================================================
-- SILVER LAYER: Normalized dimensional model
-- ============================================================================

-- Silver: Customer Dimension
CREATE TABLE IF NOT EXISTS silver.customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_seen_date DATE,
    last_seen_date DATE,
    total_bookings INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_customer_first_seen ON silver.customer(first_seen_date);
CREATE INDEX IF NOT EXISTS idx_silver_customer_last_seen ON silver.customer(last_seen_date);

-- Silver: Vehicle Type Dimension
CREATE TABLE IF NOT EXISTS silver.vehicle_type (
    vehicle_type_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver: Location Dimension
CREATE TABLE IF NOT EXISTS silver.location (
    location_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver: Booking Status Dimension
CREATE TABLE IF NOT EXISTS silver.booking_status (
    booking_status_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver: Payment Method Dimension
CREATE TABLE IF NOT EXISTS silver.payment_method (
    payment_method_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver: Booking Fact Table
CREATE TABLE IF NOT EXISTS silver.booking (
    booking_id VARCHAR(50) PRIMARY KEY,
    date DATE NOT NULL,
    time TIME NOT NULL,
    booking_value DECIMAL(10, 2),
    customer_id VARCHAR(50),
    pickup_location_id INTEGER,
    drop_location_id INTEGER,
    vehicle_type_id INTEGER,
    booking_status_id INTEGER,
    payment_method_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES silver.customer(customer_id),
    FOREIGN KEY (pickup_location_id) REFERENCES silver.location(location_id),
    FOREIGN KEY (drop_location_id) REFERENCES silver.location(location_id),
    FOREIGN KEY (vehicle_type_id) REFERENCES silver.vehicle_type(vehicle_type_id),
    FOREIGN KEY (booking_status_id) REFERENCES silver.booking_status(booking_status_id),
    FOREIGN KEY (payment_method_id) REFERENCES silver.payment_method(payment_method_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_booking_date ON silver.booking(date);
CREATE INDEX IF NOT EXISTS idx_silver_booking_customer ON silver.booking(customer_id);
CREATE INDEX IF NOT EXISTS idx_silver_booking_status ON silver.booking(booking_status_id);
CREATE INDEX IF NOT EXISTS idx_silver_booking_vehicle ON silver.booking(vehicle_type_id);

-- Silver: Ride Fact Table
CREATE TABLE IF NOT EXISTS silver.ride (
    ride_id SERIAL PRIMARY KEY,
    booking_id VARCHAR(50) UNIQUE NOT NULL,
    avg_vtat DECIMAL(10, 2),
    avg_ctat DECIMAL(10, 2),
    ride_distance DECIMAL(10, 2),
    driver_rating DECIMAL(3, 1),
    customer_rating DECIMAL(3, 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (booking_id) REFERENCES silver.booking(booking_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_ride_booking ON silver.ride(booking_id);
CREATE INDEX IF NOT EXISTS idx_silver_ride_distance ON silver.ride(ride_distance);

-- Silver: Cancelled Ride Fact Table
CREATE TABLE IF NOT EXISTS silver.cancelled_ride (
    cancellation_id SERIAL PRIMARY KEY,
    booking_id VARCHAR(50) NOT NULL,
    ride_cancelled_by VARCHAR(20),
    cancellation_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (booking_id) REFERENCES silver.booking(booking_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_cancelled_booking ON silver.cancelled_ride(booking_id);
CREATE INDEX IF NOT EXISTS idx_silver_cancelled_by ON silver.cancelled_ride(ride_cancelled_by);

-- Silver: Incompleted Ride Fact Table
CREATE TABLE IF NOT EXISTS silver.incompleted_ride (
    incompleted_id SERIAL PRIMARY KEY,
    booking_id VARCHAR(50) UNIQUE NOT NULL,
    incompletion_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (booking_id) REFERENCES silver.booking(booking_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_incompleted_booking ON silver.incompleted_ride(booking_id);

-- ============================================================================
-- GOLD LAYER: Aggregated analytics tables (optional, for future use)
-- ============================================================================

-- Gold: Daily Booking Summary
CREATE TABLE IF NOT EXISTS gold.daily_booking_summary (
    summary_date DATE PRIMARY KEY,
    total_bookings INTEGER DEFAULT 0,
    completed_rides INTEGER DEFAULT 0,
    cancelled_rides INTEGER DEFAULT 0,
    incompleted_rides INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_ride_distance DECIMAL(10, 2),
    avg_driver_rating DECIMAL(3, 1),
    avg_customer_rating DECIMAL(3, 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gold: Customer Analytics
CREATE TABLE IF NOT EXISTS gold.customer_analytics (
    customer_id VARCHAR(50) PRIMARY KEY,
    total_bookings INTEGER DEFAULT 0,
    completed_rides INTEGER DEFAULT 0,
    cancelled_rides INTEGER DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0,
    avg_rating DECIMAL(3, 1),
    favorite_vehicle_type VARCHAR(100),
    first_booking_date DATE,
    last_booking_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gold: Location Analytics
CREATE TABLE IF NOT EXISTS gold.location_analytics (
    location_id INTEGER PRIMARY KEY,
    location_name VARCHAR(255),
    total_pickups INTEGER DEFAULT 0,
    total_drops INTEGER DEFAULT 0,
    avg_booking_value DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Grant permissions on new schemas
-- ============================================================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO postgres;
