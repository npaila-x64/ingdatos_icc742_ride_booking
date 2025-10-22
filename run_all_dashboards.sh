#!/bin/bash
# Launch both Streamlit apps simultaneously

echo "🚀 Starting Ride Booking Visualization Suite..."
echo "---"
echo "📊 Dashboard: http://localhost:8501"
echo "🔍 SQL Query: http://localhost:8502"
echo "---"
echo "Press Ctrl+C to stop both servers"
echo ""

# Activate virtual environment
source venv/bin/activate

# Start dashboard in background
streamlit run streamlit_apps/dashboard.py &
DASHBOARD_PID=$!

# Wait a moment
sleep 2

# Start SQL query interface in foreground
streamlit run streamlit_apps/sql_query.py --server.port 8502

# When SQL query stops, also stop dashboard
kill $DASHBOARD_PID 2>/dev/null
