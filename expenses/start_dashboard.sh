#!/bin/bash

echo "🚀 Starting Expense Analytics Dashboard..."
echo ""
echo "Make sure you have the CSV files in the ./expense_reports directory"
echo ""

cd "$(dirname "$0")"

if [ ! -d "expense_reports" ]; then
    echo "❌ Error: expense_reports directory not found"
    echo "Please create the directory and add your CSV files"
    exit 1
fi

if [ ! -f "credit_card_expenses.py" ]; then
    echo "❌ Error: credit_card_expenses.py not found"
    exit 1
fi

echo "✅ Starting Flask server..."
echo "📊 Dashboard will be available at: http://localhost:5001"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python app.py