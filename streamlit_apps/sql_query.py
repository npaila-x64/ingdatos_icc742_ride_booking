"""
SQL Query Interface for Apache Iceberg Data

Run with: streamlit run sql_query.py
"""
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import sys
from pathlib import Path
from datetime import datetime

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.config.settings import load_settings


@st.cache_resource
def get_duckdb_connection():
    """Create DuckDB connection with Iceberg tables registered."""
    settings = load_settings()
    warehouse_path = settings.iceberg.warehouse_path
    
    # Create DuckDB connection
    con = duckdb.connect(database=':memory:', read_only=False)
    
    # Install and load Iceberg extension
    try:
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
    except Exception as e:
        st.warning(f"Iceberg extension setup: {e}")
    
    return con, warehouse_path


def register_iceberg_tables(con, warehouse_path):
    """Register Iceberg tables as DuckDB views."""
    from pyiceberg.catalog import load_catalog
    
    # Load Iceberg catalog
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": str(warehouse_path),
        }
    )
    
    # Get all tables from all namespaces
    registered_tables = []
    
    for namespace in ['bronze', 'silver', 'gold']:
        try:
            tables = catalog.list_tables(namespace)
            for table_identifier in tables:
                table_name = table_identifier[1]  # (namespace, name)
                full_name = f"{namespace}.{table_name}"
                
                try:
                    # Load table and convert to pandas
                    table = catalog.load_table(f"{namespace}.{table_name}")
                    df = table.scan().to_pandas()
                    
                    # Register as DuckDB table
                    view_name = f"{namespace}_{table_name}"
                    con.register(view_name, df)
                    registered_tables.append({
                        'layer': namespace,
                        'table': table_name,
                        'view_name': view_name,
                        'full_name': full_name,
                        'rows': len(df),
                        'columns': len(df.columns)
                    })
                except Exception as e:
                    st.warning(f"Could not load {full_name}: {e}")
        except Exception as e:
            st.warning(f"Could not list tables in {namespace}: {e}")
    
    return registered_tables


def get_sample_queries(tables_info):
    """Generate sample queries based on available tables."""
    samples = {
        "Show all gold tables": "SELECT * FROM information_schema.tables WHERE table_name LIKE 'gold%'",
        "Daily booking summary": "SELECT * FROM gold_daily_booking_summary LIMIT 100",
        "Top customers by bookings": """
SELECT customer_id, total_bookings, total_spent, avg_booking_value
FROM gold_customer_analytics
ORDER BY total_bookings DESC
LIMIT 10
""",
        "Revenue by vehicle type": """
SELECT 
    vehicle_type_name,
    SUM(total_bookings) as total_bookings,
    SUM(total_revenue) as total_revenue,
    AVG(avg_booking_value) as avg_booking_value
FROM gold_daily_booking_summary
GROUP BY vehicle_type_name
ORDER BY total_revenue DESC
""",
        "Bookings by status": """
SELECT 
    booking_status_name,
    SUM(total_bookings) as total_bookings,
    SUM(total_revenue) as total_revenue
FROM gold_daily_booking_summary
GROUP BY booking_status_name
ORDER BY total_bookings DESC
""",
        "Top locations by activity": """
SELECT name, pickups, dropoffs, total_activity, avg_booking_value
FROM gold_location_analytics
ORDER BY total_activity DESC
LIMIT 15
""",
        "Daily trends": """
SELECT 
    date,
    SUM(total_bookings) as daily_bookings,
    SUM(total_revenue) as daily_revenue
FROM gold_daily_booking_summary
GROUP BY date
ORDER BY date DESC
""",
        "Silver layer - All customers": "SELECT * FROM silver_customer LIMIT 100",
        "Bronze layer - Raw bookings": "SELECT * FROM bronze_booking LIMIT 50",
    }
    
    # Filter samples based on available tables
    available_samples = {}
    for name, query in samples.items():
        available_samples[name] = query
    
    return available_samples


def execute_query(con, query):
    """Execute SQL query and return results."""
    try:
        result = con.execute(query).fetchdf()
        return result, None
    except Exception as e:
        return None, str(e)


def visualize_results(df, query):
    """Auto-generate visualizations for query results."""
    if df is None or len(df) == 0:
        return
    
    # Check for common patterns and suggest visualizations
    numeric_cols = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns.tolist()
    
    if len(df) > 0 and len(numeric_cols) > 0:
        st.markdown("### 📊 Visualization Options")
        
        viz_type = st.radio(
            "Select visualization",
            ["None", "Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot"],
            horizontal=True
        )
        
        if viz_type != "None":
            col1, col2 = st.columns(2)
            
            with col1:
                x_col = st.selectbox("X-axis", df.columns.tolist(), key="x_axis")
            
            with col2:
                if viz_type != "Pie Chart":
                    y_col = st.selectbox("Y-axis", numeric_cols, key="y_axis")
                else:
                    y_col = st.selectbox("Values", numeric_cols, key="y_axis")
            
            # Create visualization
            try:
                if viz_type == "Bar Chart":
                    fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif viz_type == "Line Chart":
                    fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}", markers=True)
                    st.plotly_chart(fig, use_container_width=True)
                
                elif viz_type == "Pie Chart":
                    fig = px.pie(df, names=x_col, values=y_col, title=f"{y_col} distribution by {x_col}")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif viz_type == "Scatter Plot":
                    fig = px.scatter(df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")
                    st.plotly_chart(fig, use_container_width=True)
            
            except Exception as e:
                st.error(f"Visualization error: {e}")


def main():
    st.set_page_config(
        page_title="SQL Query Interface",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🔍 SQL Query Interface")
    st.markdown("Query your Apache Iceberg data using SQL")
    st.markdown("---")
    
    # Initialize connection
    con, warehouse_path = get_duckdb_connection()
    
    # Sidebar
    with st.sidebar:
        st.header("📚 Available Tables")
        
        if st.button("🔄 Refresh Tables", help="Reload tables from Iceberg catalog"):
            st.cache_resource.clear()
            st.rerun()
        
        with st.spinner("Loading Iceberg tables..."):
            tables_info = register_iceberg_tables(con, warehouse_path)
        
        if tables_info:
            st.success(f"✅ Loaded {len(tables_info)} tables")
            
            # Group by layer
            for layer in ['gold', 'silver', 'bronze']:
                layer_tables = [t for t in tables_info if t['layer'] == layer]
                if layer_tables:
                    with st.expander(f"🥇 {layer.upper()} ({len(layer_tables)} tables)", expanded=(layer == 'gold')):
                        for table in layer_tables:
                            st.markdown(f"**`{table['view_name']}`**")
                            st.caption(f"📊 {table['rows']:,} rows × {table['columns']} cols")
        else:
            st.warning("No tables found")
        
        st.markdown("---")
        st.info("💡 **Tip**: Use table names like `gold_daily_booking_summary` in your queries")
    
    # Main content
    sample_queries = get_sample_queries(tables_info)
    
    # Sample queries selector
    st.subheader("📝 Quick Start")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        selected_sample = st.selectbox(
            "Choose a sample query",
            ["Custom Query"] + list(sample_queries.keys())
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📋 View Schema", help="Show table schemas"):
            with st.expander("📋 Table Schemas", expanded=True):
                for table in tables_info:
                    st.markdown(f"**{table['view_name']}**")
                    try:
                        schema_df = con.execute(f"DESCRIBE {table['view_name']}").fetchdf()
                        st.dataframe(schema_df, use_container_width=True, hide_index=True)
                    except:
                        st.caption("Schema unavailable")
    
    # Query editor
    st.subheader("✍️ SQL Query")
    
    if selected_sample == "Custom Query":
        default_query = "-- Write your SQL query here\nSELECT * FROM gold_daily_booking_summary LIMIT 10"
    else:
        default_query = sample_queries[selected_sample]
    
    query = st.text_area(
        "Enter your SQL query",
        value=default_query,
        height=200,
        help="Write SQL queries using DuckDB syntax"
    )
    
    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        execute_btn = st.button("▶️ Execute Query", type="primary", use_container_width=True)
    
    with col2:
        if st.button("🗑️ Clear", use_container_width=True):
            st.rerun()
    
    # Execute query
    if execute_btn and query.strip():
        st.markdown("---")
        st.subheader("📊 Query Results")
        
        with st.spinner("Executing query..."):
            start_time = datetime.now()
            result_df, error = execute_query(con, query)
            execution_time = (datetime.now() - start_time).total_seconds()
        
        if error:
            st.error(f"❌ Query Error: {error}")
        elif result_df is not None:
            # Show metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Rows Returned", f"{len(result_df):,}")
            with col2:
                st.metric("Columns", len(result_df.columns))
            with col3:
                st.metric("Execution Time", f"{execution_time:.3f}s")
            
            # Show results
            st.dataframe(result_df, use_container_width=True, height=400)
            
            # Download button
            csv = result_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
            # Visualizations
            if len(result_df) > 0:
                visualize_results(result_df, query)
    
    # Footer
    st.markdown("---")
    st.caption("🔍 SQL Query Interface | Powered by DuckDB + Apache Iceberg")


if __name__ == "__main__":
    main()
