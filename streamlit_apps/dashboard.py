"""
Streamlit Dashboard for Ride Booking Analytics (Apache Iceberg)

Run with: streamlit run dashboard.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyiceberg.catalog import load_catalog
from datetime import datetime
import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.config.settings import load_settings


@st.cache_resource
def get_catalog():
    """Load Iceberg catalog (cached)"""
    settings = load_settings()
    warehouse_path = settings.iceberg.warehouse_path
    
    # Ensure warehouse exists
    warehouse_path.mkdir(parents=True, exist_ok=True)
    
    # Load catalog with same configuration as ETL
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": str(warehouse_path),
        }
    )
    return catalog


def load_table_data(layer: str, table_name: str, extraction_month: str = "All") -> pd.DataFrame:
    """Load data from Iceberg table with optional extraction_month filter
    
    Note: Cache is intentionally removed to ensure filter changes are reflected immediately.
    For Silver/Gold layers without extraction_month, we filter via bronze booking_id.
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table(f"{layer}.{table_name}")
        df = table.scan().to_pandas()
        
        # Fix timestamp columns for Arrow compatibility
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if it contains timestamp-like objects
                if len(df) > 0 and isinstance(df[col].iloc[0], pd.Timestamp):
                    df[col] = pd.to_datetime(df[col])
                elif df[col].apply(lambda x: isinstance(x, pd.Timestamp)).any():
                    df[col] = pd.to_datetime(df[col])
        
        # Debug logging
        total_rows_before = len(df)
        
        # Apply extraction_month filter if specified
        if extraction_month != "All":
            if 'extraction_month' in df.columns:
                # Bronze layer: Direct filter
                df = df[df['extraction_month'] == extraction_month]
                st.sidebar.caption(f"✓ Filtered {layer}.{table_name}: {total_rows_before} → {len(df)} rows")
            else:
                # Silver/Gold layer: Filter using bronze.booking as source of truth
                try:
                    bronze_booking = catalog.load_table("bronze.booking")
                    bronze_df = bronze_booking.scan().to_pandas()
                    
                    if 'extraction_month' in bronze_df.columns:
                        bronze_filtered = bronze_df[bronze_df['extraction_month'] == extraction_month]
                        
                        # Apply filter based on table structure
                        if 'booking_id' in df.columns:
                            # Direct booking_id reference
                            valid_booking_ids = set(bronze_filtered['booking_id'])
                            df = df[df['booking_id'].isin(valid_booking_ids)]
                            st.sidebar.caption(f"✓ Filtered {layer}.{table_name} via booking_id: {total_rows_before} → {len(df)} rows")
                        elif 'customer_id' in df.columns and layer == "gold" and table_name == "customer_analytics":
                            # Customer analytics: filter by customers who have bookings in this extraction
                            valid_customer_ids = set(bronze_filtered['customer_id'])
                            df = df[df['customer_id'].isin(valid_customer_ids)]
                            st.sidebar.caption(f"✓ Filtered {layer}.{table_name} via customer_id: {total_rows_before} → {len(df)} rows")
                        elif layer == "gold" and table_name == "location_analytics":
                            # Location analytics: filter by locations used in this extraction
                            # bronze.booking uses 'pickup_location' and 'drop_location' (names, not IDs)
                            # But location_analytics might use location_id or name
                            if 'location_id' in df.columns:
                                # If using IDs, skip filter (can't map name to ID easily)
                                st.sidebar.warning(f"⚠️ {layer}.{table_name}: Using all data (ID-based)")
                            elif 'name' in df.columns:
                                valid_locations = set(bronze_filtered['pickup_location']) | set(bronze_filtered['drop_location'])
                                df = df[df['name'].isin(valid_locations)]
                                st.sidebar.caption(f"✓ Filtered {layer}.{table_name} via name: {total_rows_before} → {len(df)} rows")
                        elif layer == "gold" and table_name == "daily_booking_summary":
                            # Daily summary: This should be recalculated, not filtered here
                            st.sidebar.info(f"ℹ️ {layer}.{table_name}: Recalculated above")
                        else:
                            st.sidebar.warning(f"⚠️ {layer}.{table_name}: Using all data (no filter mapping)")
                except Exception as e:
                    st.sidebar.error(f"⚠️ {layer}.{table_name}: Filter error: {str(e)}")
        
        return df
    except Exception as e:
        st.error(f"Error loading {layer}.{table_name}: {str(e)}")
        return pd.DataFrame()


def get_available_extraction_months() -> list:
    """Get list of available extraction months from bronze data
    
    Note: Cache removed to ensure fresh data on each load.
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table("bronze.booking")
        df = table.scan().to_pandas()
        if 'extraction_month' in df.columns:
            return sorted(df['extraction_month'].unique(), reverse=True)
        return []
    except:
        return []


def main():
    st.set_page_config(
        page_title="Ride Booking Analytics",
        page_icon="🚗",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("🚗 Ride Booking Analytics Dashboard")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("📊 Data Layers")
        layer = st.radio(
            "Select Layer",
            ["Gold (Analytics)", "Silver (Dimensional)", "Bronze (Raw)"],
            help="Choose which data layer to explore"
        )
        
        st.markdown("---")
        
        # Extraction Date Filter
        st.header("📅 Data Filter")
        
        # Get available extraction months
        available_months = get_available_extraction_months()
        
        if len(available_months) > 1:
            st.warning(f"⚠️ Multiple extractions detected: {len(available_months)}")
        
        if available_months:
            selected_month = st.selectbox(
                "Extraction Month",
                options=["All"] + available_months,
                index=1 if len(available_months) > 0 else 0,
                key="extraction_month_filter",
                help="Filter data by when it was extracted into the warehouse"
            )
            
            if selected_month != "All":
                st.info(f"📊 Showing data from extraction: **{selected_month}**")
        else:
            selected_month = "All"
            st.info("No extraction_month data available")
        
        st.markdown("---")
        
        # Clear cache button
        if st.button("🔄 Refresh Data", help="Clear cache and reload data"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.info("**Medallion Architecture**\n\n"
                "🥉 **Bronze**: Raw data\n\n"
                "🥈 **Silver**: Cleaned & normalized\n\n"
                "🥇 **Gold**: Business metrics")

    # Debug info - remove this after confirming it works
    st.caption(f"🔍 Debug: Current filter = `{selected_month}` | Layer = `{layer}`")

    # Gold Layer - Analytics Dashboard
    if "Gold" in layer:
        st.header("🥇 Gold Layer - Business Analytics")
        
        # For gold layer with extraction_month filter, recalculate from bronze
        if selected_month != "All":
            # Recalculate daily_summary from filtered bronze data
            try:
                catalog = get_catalog()
                bronze_booking = catalog.load_table("bronze.booking").scan().to_pandas()
                bronze_booking = bronze_booking[bronze_booking['extraction_month'] == selected_month]
                
                # The bronze.booking already has the names directly (no IDs!)
                # date column exists, time column for timestamp
                # Aggregate
                daily_summary = bronze_booking.groupby(['date', 'vehicle_type', 'booking_status']).agg({
                    'booking_id': 'count',
                    'booking_value': 'sum'
                }).reset_index()
                
                daily_summary.columns = ['date', 'vehicle_type_name', 'booking_status_name', 'total_bookings', 'total_revenue']
                daily_summary['avg_booking_value'] = daily_summary['total_revenue'] / daily_summary['total_bookings']
                
                st.sidebar.success(f"✓ Recalculated daily_summary from {len(bronze_booking)} filtered bookings")
            except Exception as e:
                st.sidebar.error(f"Error recalculating daily_summary: {e}")
                daily_summary = pd.DataFrame()
        else:
            # Load gold tables directly when no filter
            daily_summary = load_table_data("gold", "daily_booking_summary", selected_month)
        
        customer_analytics = load_table_data("gold", "customer_analytics", selected_month)
        location_analytics = load_table_data("gold", "location_analytics", selected_month)
        
        if not daily_summary.empty:
            # Metrics row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_bookings = daily_summary['total_bookings'].sum()
                st.metric("Total Bookings", f"{total_bookings:,}")
            
            with col2:
                total_revenue = daily_summary['total_revenue'].sum()
                st.metric("Total Revenue", f"${total_revenue:,.2f}")
            
            with col3:
                # Calculate weighted average: total_revenue / total_bookings
                # NOT the mean of daily averages (which would be unweighted)
                avg_booking = total_revenue / total_bookings if total_bookings > 0 else 0
                st.metric("Avg Booking Value", f"${avg_booking:.2f}")
            
            with col4:
                unique_dates = daily_summary['date'].nunique()
                st.metric("Date Range", f"{unique_dates} days")
            
            st.markdown("---")
            
            # Charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Daily Booking Trends")
                # Aggregate by date
                daily_trend = daily_summary.groupby('date').agg({
                    'total_bookings': 'sum'
                }).reset_index()
                fig = px.line(
                    daily_trend,
                    x='date',
                    y='total_bookings',
                    title='Bookings Over Time',
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("💰 Revenue Trends")
                # Aggregate by date
                daily_revenue = daily_summary.groupby('date').agg({
                    'total_revenue': 'sum'
                }).reset_index()
                fig = px.area(
                    daily_revenue,
                    x='date',
                    y='total_revenue',
                    title='Revenue Over Time'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # More charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🚖 Booking Status Breakdown")
                status_data = daily_summary.groupby('booking_status_name').agg({
                    'total_bookings': 'sum'
                }).reset_index()
                fig = px.pie(
                    status_data,
                    values='total_bookings',
                    names='booking_status_name',
                    title='Booking Status Distribution'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("🚙 Vehicle Types")
                vehicle_data = daily_summary.groupby('vehicle_type_name').agg({
                    'total_bookings': 'sum'
                }).reset_index()
                fig = px.bar(
                    vehicle_data,
                    x='vehicle_type_name',
                    y='total_bookings',
                    title='Bookings by Vehicle Type',
                    labels={'vehicle_type_name': 'Vehicle Type', 'total_bookings': 'Bookings'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # Customer Analytics
        if not customer_analytics.empty:
            st.markdown("---")
            st.subheader("👥 Top Customers")
            
            # Sort by total bookings
            top_customers = customer_analytics.nlargest(10, 'total_bookings')
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    top_customers,
                    x='customer_id',
                    y='total_bookings',
                    title='Top 10 Customers by Bookings',
                    labels={'customer_id': 'Customer', 'total_bookings': 'Bookings'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    top_customers,
                    x='customer_id',
                    y='total_spent',
                    title='Top 10 Customers by Revenue',
                    labels={'customer_id': 'Customer', 'total_spent': 'Revenue ($)'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Location Analytics
        if not location_analytics.empty:
            st.markdown("---")
            st.subheader("📍 Location Analytics")
            
            top_locations = location_analytics.nlargest(10, 'total_activity')
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    top_locations,
                    x='name',
                    y='pickups',
                    title='Top 10 Pickup Locations',
                    labels={'name': 'Location', 'pickups': 'Pickups'}
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    top_locations,
                    x='name',
                    y='dropoffs',
                    title='Top 10 Dropoff Locations',
                    labels={'name': 'Location', 'dropoffs': 'Dropoffs'}
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)

    # Silver Layer
    elif "Silver" in layer:
        st.header("🥈 Silver Layer - Dimensional Data")
        
        table_options = [
            "booking", "customer", "location", "ride", 
            "vehicle_type", "payment_method", "booking_status",
            "cancelled_ride", "incompleted_ride"
        ]
        
        selected_table = st.selectbox("Select Table", table_options)
        
        df = load_table_data("silver", selected_table, selected_month)
        
        if not df.empty:
            st.subheader(f"📋 {selected_table.replace('_', ' ').title()} Data")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", f"{len(df):,}")
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                st.metric("Memory Usage", f"{memory_mb:.2f} MB")
            
            # Show data
            st.dataframe(df, width='stretch', height=400)
            
            # Column statistics
            with st.expander("📊 Column Statistics"):
                st.write(df.describe())

    # Bronze Layer
    else:
        st.header("🥉 Bronze Layer - Raw Data")
        
        table_options = [
            "booking", "customer", "location", "ride", 
            "vehicle_type", "payment_method", "booking_status",
            "cancelled_ride", "incompleted_ride"
        ]
        
        selected_table = st.selectbox("Select Table", table_options)
        
        df = load_table_data("bronze", selected_table, selected_month)
        
        if not df.empty:
            st.subheader(f"📋 {selected_table.replace('_', ' ').title()} Raw Data")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", f"{len(df):,}")
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                st.metric("Memory Usage", f"{memory_mb:.2f} MB")
            
            # Show data
            st.dataframe(df, width='stretch', height=400)
            
            # Show schema
            with st.expander("🔍 Table Schema"):
                schema_df = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.astype(str),
                    'Non-Null Count': df.count().values,
                    'Null Count': df.isna().sum().values
                })
                st.dataframe(schema_df, width='stretch')

    # Footer
    st.markdown("---")
    st.caption("🚗 Ride Booking Analytics | Powered by Apache Iceberg + Streamlit")


if __name__ == "__main__":
    main()
