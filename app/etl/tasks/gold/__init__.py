"""Gold layer granular aggregation tasks for analytics tables."""

from .daily_booking_summary import aggregate_gold_daily_booking_summary
from .customer_analytics import aggregate_gold_customer_analytics
from .location_analytics import aggregate_gold_location_analytics
from .cancellation_rate import aggregate_gold_cancellation_rate
from .incomplete_rate import aggregate_gold_incomplete_rate
from .avg_revenue_per_ride import aggregate_gold_avg_revenue_per_ride
from .user_frequency import aggregate_gold_user_frequency
from .monthly_retention import aggregate_gold_monthly_retention
from .avg_wait_time import aggregate_gold_avg_wait_time

__all__ = [
    'aggregate_gold_daily_booking_summary',
    'aggregate_gold_customer_analytics',
    'aggregate_gold_location_analytics',
    'aggregate_gold_cancellation_rate',
    'aggregate_gold_incomplete_rate',
    'aggregate_gold_avg_revenue_per_ride',
    'aggregate_gold_user_frequency',
    'aggregate_gold_monthly_retention',
    'aggregate_gold_avg_wait_time',
]

