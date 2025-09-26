# 📊 Expense Analytics Dashboard

A beautiful, interactive web dashboard for analyzing credit card transaction categorization and spending patterns.

## ✨ Features

- **Interactive Charts**: Monthly spending trends with multiple visualization types (line, bar, area)
- **Category Analytics**: Pie chart showing spending distribution across categories
- **Statistics Cards**: Key metrics including total spending, averages, and trends
- **Transaction Search**: Real-time search through all transactions
- **Responsive Design**: Beautiful UI that works on desktop and mobile
- **Automatic Categorization**: Smart categorization based on merchant names

## 🚀 Quick Start

1. **Make sure your CSV files are in the `expense_reports/` directory**
2. **Start the dashboard**:
   ```bash
   ./start_dashboard.sh
   ```
   Or manually:
   ```bash
   python app.py
   ```
3. **Open your browser** and go to: http://localhost:5000

## 📁 File Structure

```
expenses/
├── app.py                  # Flask web server
├── credit_card_expenses.py # Data processing logic
├── start_dashboard.sh      # Startup script
├── templates/
│   └── dashboard.html      # Main dashboard interface
└── expense_reports/        # Your CSV files go here
    ├── Export pohybov 1.csv
    ├── Export pohybov 2.csv
    └── ...
```

## 🎨 Dashboard Features

### 📈 Monthly Trends Chart
- Toggle between line, bar, and area chart types
- Shows total spending and top 3 categories over time
- Interactive hover details
- Responsive design

### 🥧 Category Distribution
- Beautiful pie chart showing spending by category
- Automatic color coding
- Hover for detailed amounts and percentages

### 📊 Statistics Cards
- Total spending across all periods
- Average monthly spending with trend indicators
- Top spending category
- Number of months tracked

### 🔍 Transaction Table
- Real-time search functionality
- Color-coded category badges
- Recent transactions displayed first
- Responsive table design

## 🏷️ Categories

The dashboard automatically categorizes transactions based on merchant names:

- **Groceries**: Supermarkets, food stores
- **Eating Out**: Restaurants, cafes, fast food
- **Transportation**: Bolt, taxis
- **Car Care**: Car washes, gas stations
- **Furniture**: IKEA, home improvement stores
- **Travel**: Flights, hotels, accommodation
- **Parking**: Parking meters and services
- **Other**: Uncategorized transactions

## 🔧 Customization

### Adding New Categories
Edit the `CATEGORIES` dictionary in `credit_card_expenses.py`:

```python
CATEGORIES = {
    "your_category": ["keyword1", "keyword2", "merchant_name"],
    # ... existing categories
}
```

### Styling
The dashboard uses modern CSS with gradients and animations. Colors and styles can be customized in the `<style>` section of `dashboard.html`.

## 📊 API Endpoints

The Flask server provides several API endpoints:

- `GET /` - Main dashboard page
- `GET /api/monthly-data` - Monthly spending data for charts
- `GET /api/category-totals` - Total spending by category
- `GET /api/transactions` - All transactions with search capability
- `GET /api/trends` - Trend analysis data
- `GET /api/categories` - Available categories and keywords

## 🛠️ Technical Stack

- **Backend**: Flask (Python)
- **Frontend**: Vanilla JavaScript, HTML5, CSS3
- **Charts**: Plotly.js for interactive visualizations
- **Styling**: Modern CSS with gradients and animations
- **Icons**: Font Awesome
- **Fonts**: Inter (Google Fonts)

## 💡 Tips

- **Search**: Use the search box to quickly find specific merchants or categories
- **Chart Types**: Switch between different chart types for better data visualization
- **Mobile**: The dashboard is fully responsive and works great on mobile devices
- **Performance**: The dashboard loads quickly and handles large datasets efficiently

## 🚨 Troubleshooting

1. **No data showing**: Make sure CSV files are in the `expense_reports/` directory
2. **Server won't start**: Check that Flask and Flask-CORS are installed (`poetry install`)
3. **Categories not working**: Verify merchant names match keywords in `CATEGORIES`
4. **Charts not loading**: Ensure internet connection for CDN resources (Plotly.js, Font Awesome)

Enjoy analyzing your expenses! 🎉