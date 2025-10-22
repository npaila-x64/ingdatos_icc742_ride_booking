#!/bin/bash
# Launch SQL Query Interface for Ride Booking Analytics

echo "🔍 Starting SQL Query Interface..."
echo "---"
echo "Query interface will open in your browser at: http://localhost:8502"
echo "Press Ctrl+C to stop the server"
echo "---"

streamlit run streamlit_apps/sql_query.py --server.port 8502
