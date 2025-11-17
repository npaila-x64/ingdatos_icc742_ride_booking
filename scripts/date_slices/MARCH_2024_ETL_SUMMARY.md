# March 2024 ETL Execution Summary

**Execution Date**: October 22, 2025  
**Data Period**: March 1 - March 31, 2024  

---

## 📊 Execution Results

### Bronze Layer (Raw Data Extraction)
Data partitioned by `extraction_month='2024-03'`

| Table | Rows Written |
|-------|--------------|
| `bronze.customer` | 150,000 |
| `bronze.vehicle_type` | 149,973 |
| `bronze.location` | 300,000 |
| `bronze.booking_status` | 149,961 |
| `bronze.payment_method` | 101,990 |
| `bronze.booking` | 149,885 |
| `bronze.ride` | 92,969 |
| `bronze.cancelled_ride` | 37,492 |
| `bronze.incompleted_ride` | 8,999 |
| **Total** | **1,012,270** |

**March 2024 Specific Data**:
- Bookings in March 2024 partition: **25,412**
- Date range: **2024-03-01** to **2024-03-31**

### Silver Layer (Normalized Data)
Data cleaned and normalized with dimensional model

| Table | Rows Written |
|-------|--------------|
| `silver.customer` | 148,678 |
| `silver.vehicle_type` | 7 |
| `silver.location` | 176 |
| `silver.booking_status` | 5 |
| `silver.payment_method` | 5 |
| `silver.booking` | 299,770 |
| `silver.ride` | 185,938 |
| `silver.cancelled_ride` | 74,984 |
| `silver.incompleted_ride` | 17,998 |
| **Total** | **727,561** |

### Gold Layer (Analytics & Aggregations)
Pre-aggregated analytics for business intelligence

| Table | Rows Written |
|-------|--------------|
| `gold.daily_booking_summary` | 12,022 |
| `gold.customer_analytics` | 148,678 |
| `gold.location_analytics` | 176 |
| **Total** | **160,876** |

---

## 📈 March 2024 Business Insights

### Overall Performance
- **Total Bookings**: 25,412
- **Total Revenue**: ₹9,129,690.00
- **Average Booking Value**: ₹359.27
- **Daily Summaries**: 1,015 records (31 days × vehicle types × booking statuses)

### Top Vehicle Types by Revenue (March 2024)
1. **Auto**: ₹2,313,322.00 (25.3%)
2. **Go Mini**: ₹1,764,672.00 (19.3%)
3. **Go Sedan**: ₹1,633,116.00 (17.9%)
4. **Bike**: ₹1,377,504.00 (15.1%)
5. **Premier Sedan**: ₹1,094,902.00 (12.0%)

### Vehicle Types Available
1. eBike (ID: 1)
2. Go Sedan (ID: 2)
3. Auto (ID: 3)
4. Premier Sedan (ID: 4)
5. Bike (ID: 5)
6. Go Mini (ID: 6)
7. Uber XL (ID: 7)

### Top 10 Most Active Locations (Overall)
| Location | Pickups | Dropoffs | Total Activity |
|----------|---------|----------|----------------|
| Madipur | 1,836 | 1,804 | 3,640 |
| Khandsa | 1,898 | 1,732 | 3,630 |
| Ashram | 1,746 | 1,870 | 3,616 |
| Udyog Vihar | 1,790 | 1,810 | 3,600 |
| Lok Kalyan Marg | 1,758 | 1,832 | 3,590 |
| Nehru Place | 1,770 | 1,804 | 3,574 |
| Basai Dhankot | 1,724 | 1,832 | 3,556 |
| Saket | 1,862 | 1,690 | 3,552 |
| Cyber Hub | 1,726 | 1,824 | 3,550 |
| New Colony | 1,762 | 1,780 | 3,542 |

### Top 10 Customers by Total Spend (Overall)
| Customer ID | Bookings | Total Spent | Avg Booking |
|-------------|----------|-------------|-------------|
| CID2674107 | 4 | ₹9,974 | ₹2,494 |
| CID7828101 | 6 | ₹9,444 | ₹1,574 |
| CID2706299 | 2 | ₹8,554 | ₹4,277 |
| CID4843078 | 2 | ₹8,456 | ₹4,228 |
| CID2978596 | 2 | ₹8,440 | ₹4,220 |
| CID5235759 | 2 | ₹8,404 | ₹4,202 |
| CID5789715 | 2 | ₹8,266 | ₹4,133 |
| CID9539119 | 2 | ₹8,218 | ₹4,109 |
| CID1753183 | 2 | ₹8,176 | ₹4,088 |
| CID8107092 | 2 | ₹8,120 | ₹4,060 |

---

## 📁 Data Partitions in Warehouse

All historical data now includes 12 months (Jan-Dec 2024):

| Month | Bookings |
|-------|----------|
| 2024-01 | 25,710 |
| 2024-02 | 23,840 |
| **2024-03** | **25,412** ← New |
| 2024-04 | 24,384 |
| 2024-05 | 25,536 |
| 2024-06 | 24,874 |
| 2024-07 | 25,778 |
| 2024-08 | 25,248 |
| 2024-09 | 24,474 |
| 2024-10 | 25,276 |
| 2024-11 | 24,764 |
| 2024-12 | 24,474 |

---

## 🔍 How to Query March 2024 Data

### Python Example
```python
from app.adapters.iceberg_adapter import IcebergAdapter
from app.config.settings import load_settings
import pandas as pd

# Initialize
settings = load_settings()
iceberg = IcebergAdapter(settings.iceberg)

# Query Bronze - March 2024 partition
booking_bronze = iceberg.read_table('bronze', 'booking')
march_bronze = booking_bronze[booking_bronze['extraction_month'] == '2024-03']
print(f"March 2024 bookings: {len(march_bronze):,}")

# Query by actual date range
booking_bronze['date'] = pd.to_datetime(booking_bronze['date'])
march_dates = booking_bronze[
    (booking_bronze['date'] >= '2024-03-01') & 
    (booking_bronze['date'] < '2024-04-01')
]

# Query Gold analytics for March
daily_summary = iceberg.read_table('gold', 'daily_booking_summary')
daily_summary['date'] = pd.to_datetime(daily_summary['date'])
march_summary = daily_summary[
    (daily_summary['date'] >= '2024-03-01') & 
    (daily_summary['date'] < '2024-04-01')
]

# Calculate March metrics
march_revenue = march_summary['total_revenue'].sum()
march_bookings = march_summary['total_bookings'].sum()
print(f"March revenue: ₹{march_revenue:,.2f}")
print(f"March bookings: {march_bookings:,.0f}")
```

### Using PyIceberg (SQL-like)
```python
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

catalog = load_catalog('default', warehouse='file://./warehouse')
table = catalog.load_table('bronze.booking')

# Filter with predicate pushdown
march_data = table.scan(
    row_filter=EqualTo("extraction_month", "2024-03")
).to_pandas()
```

---

## 📂 Files Created

### Execution Scripts
- **`run_march_2024_etl.py`** - Main ETL execution script for March 2024
- **`verify_march_2024.py`** - Verification and analysis script
- **`MARCH_2024_ETL_SUMMARY.md`** - This summary document

### Data Location
- **Warehouse Path**: `./warehouse/`
- **Bronze Tables**: `./warehouse/bronze/` (partitioned by `extraction_month`)
- **Silver Tables**: `./warehouse/silver/`
- **Gold Tables**: `./warehouse/gold/`
