#!/bin/bash
# Start the Expense Analytics dashboard (macOS / Linux).
# On Windows, use start_dashboard.bat instead.

echo "🚀 Starting Expense Analytics Dashboard..."
echo ""

cd "$(dirname "$0")"

# Prefer python3, fall back to python.
if command -v python3 >/dev/null 2>&1; then
    PY=python3
elif command -v python >/dev/null 2>&1; then
    PY=python
else
    echo "❌ Error: Python 3 not found. Install it from https://www.python.org/downloads/"
    exit 1
fi

if [ ! -f "app.py" ]; then
    echo "❌ Error: app.py not found (run this from the expenses/ directory)."
    exit 1
fi

# No expense_reports/ yet? That's fine — the app offers sample data on first run.
if [ ! -d "expense_reports" ] || [ -z "$(ls -A expense_reports/*.csv 2>/dev/null)" ]; then
    echo "ℹ️  No CSV exports in ./expense_reports — you can drop your bank exports there,"
    echo "   or click \"Explore with sample data\" in the app to try it with no setup."
    echo ""
fi

echo "✅ Starting Flask server..."
echo "📊 Dashboard will be available at: http://localhost:5001"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

"$PY" app.py
