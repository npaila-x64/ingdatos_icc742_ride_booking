"""Iceberg table schema definitions for the medallion architecture."""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

# Bronze Layer Schemas
BRONZE_CUSTOMER_SCHEMA = Schema(
    NestedField(1, "customer_id", StringType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "extraction_date", DateType(), required=False),
    NestedField(4, "extraction_month", StringType(), required=False),
    NestedField(5, "source_file", StringType(), required=False),
)

BRONZE_VEHICLE_TYPE_SCHEMA = Schema(
    NestedField(1, "vehicle_type_name", StringType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "extraction_date", DateType(), required=False),
    NestedField(4, "extraction_month", StringType(), required=False),
    NestedField(5, "source_file", StringType(), required=False),
)

BRONZE_LOCATION_SCHEMA = Schema(
    NestedField(1, "location_name", StringType(), required=False),
    NestedField(2, "location_type", StringType(), required=False),  # pickup or drop
    NestedField(3, "booking_id", StringType(), required=False),
    NestedField(4, "extraction_date", DateType(), required=False),
    NestedField(5, "extraction_month", StringType(), required=False),
    NestedField(6, "source_file", StringType(), required=False),
)

BRONZE_BOOKING_STATUS_SCHEMA = Schema(
    NestedField(1, "booking_status_name", StringType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "extraction_date", DateType(), required=False),
    NestedField(4, "extraction_month", StringType(), required=False),
    NestedField(5, "source_file", StringType(), required=False),
)

BRONZE_PAYMENT_METHOD_SCHEMA = Schema(
    NestedField(1, "payment_method_name", StringType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "extraction_date", DateType(), required=False),
    NestedField(4, "extraction_month", StringType(), required=False),
    NestedField(5, "source_file", StringType(), required=False),
)

BRONZE_BOOKING_SCHEMA = Schema(
    NestedField(1, "booking_id", StringType(), required=False),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "vehicle_type", StringType(), required=False),
    NestedField(4, "pickup_location", StringType(), required=False),
    NestedField(5, "drop_location", StringType(), required=False),
    NestedField(6, "booking_status", StringType(), required=False),
    NestedField(7, "payment_method", StringType(), required=False),
    NestedField(8, "booking_value", DoubleType(), required=False),
    NestedField(9, "date", DateType(), required=False),
    NestedField(10, "time", StringType(), required=False),
    NestedField(11, "extraction_date", DateType(), required=False),
    NestedField(12, "extraction_month", StringType(), required=False),
    NestedField(13, "source_file", StringType(), required=False),
)

BRONZE_RIDE_SCHEMA = Schema(
    NestedField(1, "booking_id", StringType(), required=False),
    NestedField(2, "ride_distance", DoubleType(), required=False),
    NestedField(3, "driver_rating", DoubleType(), required=False),
    NestedField(4, "customer_rating", DoubleType(), required=False),
    NestedField(5, "extraction_date", DateType(), required=False),
    NestedField(6, "extraction_month", StringType(), required=False),
    NestedField(7, "source_file", StringType(), required=False),
)

BRONZE_CANCELLED_RIDE_SCHEMA = Schema(
    NestedField(1, "booking_id", StringType(), required=False),
    NestedField(2, "cancelled_rides_by_customer", LongType(), required=False),
    NestedField(3, "cancelled_rides_by_driver", LongType(), required=False),
    NestedField(4, "extraction_date", DateType(), required=False),
    NestedField(5, "extraction_month", StringType(), required=False),
    NestedField(6, "source_file", StringType(), required=False),
)

BRONZE_INCOMPLETED_RIDE_SCHEMA = Schema(
    NestedField(1, "booking_id", StringType(), required=False),
    NestedField(2, "incomplete_rides", LongType(), required=False),
    NestedField(3, "incomplete_rides_reason", StringType(), required=False),
    NestedField(4, "extraction_date", DateType(), required=False),
    NestedField(5, "extraction_month", StringType(), required=False),
    NestedField(6, "source_file", StringType(), required=False),
)

# Silver Layer Schemas
SILVER_CUSTOMER_SCHEMA = Schema(
    NestedField(1, "customer_id", StringType(), required=False),
    NestedField(2, "first_seen_date", DateType(), required=False),
    NestedField(3, "last_seen_date", DateType(), required=False),
    NestedField(4, "total_bookings", LongType(), required=False),
    NestedField(5, "created_at", TimestampType(), required=False),
    NestedField(6, "updated_at", TimestampType(), required=False),
)

SILVER_VEHICLE_TYPE_SCHEMA = Schema(
    NestedField(1, "vehicle_type_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "created_at", TimestampType(), required=False),
    NestedField(4, "updated_at", TimestampType(), required=False),
)

SILVER_LOCATION_SCHEMA = Schema(
    NestedField(1, "location_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "created_at", TimestampType(), required=False),
    NestedField(4, "updated_at", TimestampType(), required=False),
)

SILVER_BOOKING_STATUS_SCHEMA = Schema(
    NestedField(1, "booking_status_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "created_at", TimestampType(), required=False),
    NestedField(4, "updated_at", TimestampType(), required=False),
)

SILVER_PAYMENT_METHOD_SCHEMA = Schema(
    NestedField(1, "payment_method_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "created_at", TimestampType(), required=False),
    NestedField(4, "updated_at", TimestampType(), required=False),
)

SILVER_BOOKING_SCHEMA = Schema(
    NestedField(1, "booking_id", StringType(), required=False),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "vehicle_type_id", LongType(), required=False),
    NestedField(4, "pickup_location_id", LongType(), required=False),
    NestedField(5, "drop_location_id", LongType(), required=False),
    NestedField(6, "booking_status_id", LongType(), required=False),
    NestedField(7, "payment_method_id", LongType(), required=False),
    NestedField(8, "booking_value", DoubleType(), required=False),
    NestedField(9, "date", DateType(), required=False),
    NestedField(10, "time", StringType(), required=False),
    NestedField(11, "created_at", TimestampType(), required=False),
    NestedField(12, "updated_at", TimestampType(), required=False),
)

SILVER_RIDE_SCHEMA = Schema(
    NestedField(1, "ride_id", LongType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "ride_distance", DoubleType(), required=False),
    NestedField(4, "driver_rating", DoubleType(), required=False),
    NestedField(5, "customer_rating", DoubleType(), required=False),
    NestedField(6, "created_at", TimestampType(), required=False),
    NestedField(7, "updated_at", TimestampType(), required=False),
)

SILVER_CANCELLED_RIDE_SCHEMA = Schema(
    NestedField(1, "cancellation_id", LongType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "cancelled_rides_by_customer", LongType(), required=False),
    NestedField(4, "cancelled_rides_by_driver", LongType(), required=False),
    NestedField(5, "created_at", TimestampType(), required=False),
    NestedField(6, "updated_at", TimestampType(), required=False),
)

SILVER_INCOMPLETED_RIDE_SCHEMA = Schema(
    NestedField(1, "incompleted_id", LongType(), required=False),
    NestedField(2, "booking_id", StringType(), required=False),
    NestedField(3, "incomplete_rides", LongType(), required=False),
    NestedField(4, "incomplete_rides_reason", StringType(), required=False),
    NestedField(5, "created_at", TimestampType(), required=False),
    NestedField(6, "updated_at", TimestampType(), required=False),
)

# Gold Layer Schemas
GOLD_DAILY_BOOKING_SUMMARY_SCHEMA = Schema(
    NestedField(1, "date", DateType(), required=False),
    NestedField(2, "vehicle_type_name", StringType(), required=False),
    NestedField(3, "booking_status_name", StringType(), required=False),
    NestedField(4, "total_bookings", LongType(), required=False),
    NestedField(5, "total_revenue", DoubleType(), required=False),
    NestedField(6, "avg_booking_value", DoubleType(), required=False),
    NestedField(7, "created_at", TimestampType(), required=False),
    NestedField(8, "updated_at", TimestampType(), required=False),
)

GOLD_CUSTOMER_ANALYTICS_SCHEMA = Schema(
    NestedField(1, "customer_id", StringType(), required=False),
    NestedField(2, "first_seen_date", DateType(), required=False),
    NestedField(3, "last_seen_date", DateType(), required=False),
    NestedField(4, "total_bookings", LongType(), required=False),
    NestedField(5, "total_spent", DoubleType(), required=False),
    NestedField(6, "avg_booking_value", DoubleType(), required=False),
    NestedField(7, "first_booking_date", DateType(), required=False),
    NestedField(8, "last_booking_date", DateType(), required=False),
    NestedField(9, "customer_lifetime_days", LongType(), required=False),
    NestedField(10, "created_at", TimestampType(), required=False),
    NestedField(11, "updated_at", TimestampType(), required=False),
)

GOLD_LOCATION_ANALYTICS_SCHEMA = Schema(
    NestedField(1, "location_id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "pickups", LongType(), required=False),
    NestedField(4, "dropoffs", LongType(), required=False),
    NestedField(5, "avg_booking_value", DoubleType(), required=False),
    NestedField(6, "total_activity", LongType(), required=False),
    NestedField(7, "created_at", TimestampType(), required=False),
    NestedField(8, "updated_at", TimestampType(), required=False),
)


# Partition specifications (optional, but recommended for large datasets)
def get_partition_by_month(date_field_id: int) -> PartitionSpec:
    """Create a partition spec that partitions by month."""
    return PartitionSpec(
        PartitionField(
            source_id=date_field_id,
            field_id=1000,
            transform=MonthTransform(),
            name="month",
        )
    )
