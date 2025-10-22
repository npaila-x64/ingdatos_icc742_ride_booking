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


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_table_data(layer: str, table_name: str) -> pd.DataFrame:
    """Load data from Iceberg table"""
    try:
        catalog = get_catalog()
        table = catalog.load_table(f"{layer}.{table_name}")
        df = table.scan().to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading {layer}.{table_name}: {str(e)}")
        return pd.DataFrame()


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
        st.info("**Medallion Architecture**\n\n"
                "🥉 **Bronze**: Raw data\n\n"
                "🥈 **Silver**: Cleaned & normalized\n\n"
                "🥇 **Gold**: Business metrics")

    # Gold Layer - Analytics Dashboard
    if "Gold" in layer:
        st.header("🥇 Gold Layer - Business Analytics")
        
        # Load gold tables
        daily_summary = load_table_data("gold", "daily_booking_summary")
        customer_analytics = load_table_data("gold", "customer_analytics")
        location_analytics = load_table_data("gold", "location_analytics")
        
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
                avg_booking = daily_summary['avg_booking_value'].mean()
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
                st.subheader("� Vehicle Types")
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
        
        df = load_table_data("silver", selected_table)
        
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
            st.dataframe(df, use_container_width=True, height=400)
            
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
        
        df = load_table_data("bronze", selected_table)
        
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
            st.dataframe(df, use_container_width=True, height=400)
            
            # Show schema
            with st.expander("🔍 Table Schema"):
                schema_df = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.astype(str),
                    'Non-Null Count': df.count().values,
                    'Null Count': df.isna().sum().values
                })
                st.dataframe(schema_df, use_container_width=True)

    # Footer
    st.markdown("---")
    st.caption("🚗 Ride Booking Analytics | Powered by Apache Iceberg + Streamlit")


if __name__ == "__main__":
    main()
