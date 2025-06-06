# Stock Portfolio Management System

This is a full-stack stock portfolio management system built with Vue3 (frontend), Flask (backend), and MySQL (database).

## Project Structure
```
stock_portfolio/
├── backend/
│   ├── app.py          # Flask application with API routes
│   ├── config.py       # Database and Flask configuration
│   ├── models.py       # Database models
│   └── requirements.txt# Project dependencies
└── frontend/
    ├── public/
    │   └── index.html  # Main HTML file
    ├── src/
    │   ├── main.js     # Vue3 application entry point
    │   ├── App.vue     # Main Vue component
    │   └── components/
    │       └── StockPortfolio.vue  # Stock portfolio component
    └── package.json    # Frontend project configuration
```

## Getting Started

### Backend Setup
1. Install dependencies:
```bash
pip install -r backend/requirements.txt
```

2. Create MySQL database:
```sql
CREATE DATABASE stock_portfolio;
```

3. Run the Flask application:
```bash
cd backend
python app.py
```

### Frontend Setup
1. Install dependencies:
```bash
cd frontend
npm install
```

2. Start the development server:
```bash
npm run dev
```

## API Endpoints
- `GET /api/stocks` - Get all stocks
- `GET /api/stocks/:id` - Get a specific stock by ID
- `POST /api/stocks` - Add a new stock
- `PUT /api/stocks/:id` - Update a stock
- `DELETE /api/stocks/:id` - Delete a stock

## Database Schema
```sql
CREATE TABLE `holding_stock` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ts_code` varchar(10) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `buy_price` float NOT NULL,
  `buy_date` date NOT NULL,
  `status` enum('holding','sold') DEFAULT 'holding',
  `sell_price` float DEFAULT NULL,
  `sell_date` date DEFAULT NULL,
  `reason` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;