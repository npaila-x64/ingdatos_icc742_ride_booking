"""Gold layer granular aggregation tasks for analytics tables."""

from .daily_booking_summary import aggregate_gold_daily_booking_summary
from .customer_analytics import aggregate_gold_customer_analytics
from .location_analytics import aggregate_gold_location_analytics

__all__ = [
    'aggregate_gold_daily_booking_summary',
    'aggregate_gold_customer_analytics',
    'aggregate_gold_location_analytics',
]
