#!/bin/bash
# Start the Expense Analytics dashboard (macOS / Linux).
# On Windows, use start_dashboard.bat instead.

echo "🚀 Starting Expense Analytics Dashboard..."
echo ""

cd "$(dirname "$0")"

if [ ! -f "app.py" ]; then
    echo "❌ Error: app.py not found (run this from the expenses/ directory)."
    exit 1
fi

# Prefer uv (installs deps + runs in one step). Fall back to python3/python.
if command -v uv >/dev/null 2>&1; then
    RUN=(uv run app.py)
elif command -v python3 >/dev/null 2>&1; then
    RUN=(python3 app.py)
elif command -v python >/dev/null 2>&1; then
    RUN=(python app.py)
else
    echo "❌ Error: neither uv nor Python 3 found."
    echo "   Install uv (https://docs.astral.sh/uv/) or Python (https://www.python.org/downloads/)."
    exit 1
fi

# No expense_reports/ yet? That's fine — the app offers sample data on first run.
if [ ! -d "expense_reports" ] || [ -z "$(ls -A expense_reports/*.csv 2>/dev/null)" ]; then
    echo "ℹ️  No CSV exports in ./expense_reports — you can drop your bank exports there,"
    echo "   or click \"Explore with sample data\" in the app to try it with no setup."
    echo ""
fi

echo "✅ Starting Flask server (${RUN[0]})..."
echo "📊 Dashboard will be available at: http://localhost:5001"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

"${RUN[@]}"
