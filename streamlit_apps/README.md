# Streamlit Visualization Apps

This directory contains interactive web-based visualization tools for exploring your Apache Iceberg data.

## 📊 Available Apps

### 1. **Dashboard** (`dashboard.py`)
Interactive analytics dashboard with pre-built visualizations.

**Features:**
- 🥇 **Gold Layer**: Business metrics and KPIs
  - Daily booking trends
  - Revenue analysis
  - Booking status breakdown
  - Vehicle type distribution
  - Top customers
  - Location analytics
- 🥈 **Silver Layer**: Dimensional data explorer
- 🥉 **Bronze Layer**: Raw data viewer

**Launch:**
```bash
# From project root
./run_dashboard.sh

# Or directly
streamlit run streamlit_apps/dashboard.py
```

**URL:** http://localhost:8501

---

### 2. **SQL Query Interface** (`sql_query.py`)
SQL query tool for custom data analysis.

**Features:**
- ✍️ **SQL Editor**: Write custom SQL queries
- 📚 **Sample Queries**: Pre-built query templates
- 📊 **Auto-Visualization**: Charts from query results
- 💾 **Export**: Download results as CSV
- 🔍 **Schema Viewer**: Explore table structures
- ⚡ **DuckDB Engine**: Fast analytical queries

**Launch:**
```bash
# From project root
./run_sql_query.sh

# Or directly
streamlit run streamlit_apps/sql_query.py --server.port 8502
```

**URL:** http://localhost:8502

---

## 🚀 Quick Start

### Run Both Apps Simultaneously

```bash
# Terminal 1 - Dashboard
./run_dashboard.sh

# Terminal 2 - SQL Query Interface
./run_sql_query.sh
```

### Requirements

All dependencies are already included in `pyproject.toml`:
- `streamlit>=1.28.0` - Web framework
- `plotly>=5.17.0` - Interactive visualizations
- `duckdb>=0.9.0` - SQL query engine (for sql_query.py)

---

## 📖 Usage Examples

### Dashboard
1. Open http://localhost:8501
2. Select a data layer from the sidebar (Gold/Silver/Bronze)
3. Explore pre-built visualizations
4. View metrics and trends

### SQL Query Interface
1. Open http://localhost:8502
2. Check available tables in the sidebar
3. Select a sample query or write your own
4. Execute and visualize results
5. Download data as CSV

**Sample SQL Queries:**
```sql
-- Revenue by vehicle type
SELECT 
    vehicle_type_name,
    SUM(total_revenue) as revenue,
    SUM(total_bookings) as bookings
FROM gold_daily_booking_summary
GROUP BY vehicle_type_name
ORDER BY revenue DESC;

-- Top customers
SELECT customer_id, total_bookings, total_spent
FROM gold_customer_analytics
ORDER BY total_spent DESC
LIMIT 10;

-- Location analysis
SELECT name, pickups, dropoffs, total_activity
FROM gold_location_analytics
ORDER BY total_activity DESC
LIMIT 15;
```

---

## 🔧 Configuration

Both apps automatically:
- Load Iceberg catalog from `warehouse/` directory
- Use SQLite catalog backend
- Cache data for performance (5-minute TTL)
- Connect to the same data as your ETL pipeline

---

## 📝 Notes

- **Data Refresh**: Dashboard caches data for 5 minutes. Use the refresh button or restart to see latest data.
- **Table Names**: In SQL queries, use format `{layer}_{table}` (e.g., `gold_daily_booking_summary`)
- **Performance**: First load may be slower as data is loaded into memory
- **Concurrent Usage**: Both apps can run simultaneously on different ports

---

## 🐛 Troubleshooting

**Port Already in Use:**
```bash
# Change port for dashboard
streamlit run streamlit_apps/dashboard.py --server.port 8503

# Change port for SQL query
streamlit run streamlit_apps/sql_query.py --server.port 8504
```

**Tables Not Loading:**
- Ensure ETL has run and data exists in `warehouse/`
- Check that `warehouse/catalog.db` exists
- Verify Iceberg tables with: `python -m app.etl.cli verify`

**Cache Issues:**
- Click "Refresh Tables" in SQL interface
- Restart the Streamlit app
- Clear browser cache
