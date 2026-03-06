# ChoraBackend - Arbitrage Opportunities API

A Flask application that exposes a POST API endpoint to retrieve top arbitrage opportunities from the `market_matching` table.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file in the root directory with your database URL:
```
DATABASE_URL=postgresql://username:password@host:port/database
PORT=5000
```

3. Run the application:

**Development mode:**
```bash
python app.py
```

**Production mode with Gunicorn:**
```bash
gunicorn -c gunicorn_config.py wsgi:application
```

Or with custom settings:
```bash
gunicorn --bind 0.0.0.0:5000 --workers 4 wsgi:application
```

The application will start on `http://0.0.0.0:5000` (or the port specified in your `.env` file).

### Gunicorn Configuration

The `gunicorn_config.py` file contains production-ready settings:
- **Workers**: Automatically set to `(CPU cores * 2) + 1` (can be overridden with `GUNICORN_WORKERS` env var)
- **Port**: Uses `PORT` environment variable (default: 5000)
- **Logging**: Logs to stdout/stderr (can be configured with `GUNICORN_ACCESS_LOG` and `GUNICORN_ERROR_LOG`)

You can customize these settings by modifying `gunicorn_config.py` or setting environment variables.

## API Endpoints

### POST `/arbitrage-opportunities`

Retrieves top arbitrage opportunities from the `market_matching` table.

**Request Body** (JSON, all fields optional):
```json
{
  "num_markets": 10,
  "min_arb_price": 0.5,
  "max_arb_price": 0.05
}
```

**Parameters:**
- `num_markets` (integer, optional): Number of markets to return. Default: 10
- `min_arb_price` (float, optional): Minimum arbitrage price filter. Default: 0.5
- `max_arb_price` (float, optional): Maximum arbitrage price filter. Default: 0.05

**Response:**
```json
{
  "success": true,
  "count": 10,
  "opportunities": [
    {
      "id": 1,
      "arb_price": 0.75,
      "confidence": 0.95,
      "kalshi_market_id": "market_123",
      "kalshi_market_title": "Will X happen?",
      "kalshi_market_yes_price": 0.60,
      "kalshi_market_no_price": 0.40,
      "kalshi_market_volume": 10000,
      "kalshi_market_open_interest": 5000,
      "kalshi_market_status": "open",
      "kalshi_market_subtitle": "Yes",
      "kalshi_market_no_subtitle": "No",
      "kalshi_market_rules_primary": "Market rules...",
      "kalshi_event_id": "event_123",
      "kalshi_event_title": "Event Title",
      "kalshi_event_category": "Politics",
      "kalshi_event_sub_category": "Elections",
      "kalshi_event_close_time": "2024-12-31T23:59:59+00:00",
      "kalshi_event_status": "open",
      "polymarket_market_id": "market_456",
      "polymarket_market_title": "Will X happen?",
      "polymarket_market_yes_price": 0.65,
      "polymarket_market_no_price": 0.35,
      "polymarket_market_liquidity": 20000,
      "polymarket_market_volume": 15000,
      "polymarket_market_status": "open",
      "polymarket_market_description": "Market description...",
      "polymarket_event_id": "event_456",
      "polymarket_event_title": "Event Title",
      "polymarket_event_category": "Politics",
      "polymarket_event_close_time": "2024-12-31T23:59:59",
      "polymarket_event_status": "open"
    },
    ...
  ]
}
```

**Note:** Fields will be `null` if the corresponding data is not found in the database.

**Example Request:**
```bash
curl -X POST http://localhost:5000/arbitrage-opportunities \
  -H "Content-Type: application/json" \
  -d '{
    "num_markets": 5,
    "min_arb_price": 0.6,
    "max_arb_price": 0.9
  }'
```

### GET `/health`

Health check endpoint to verify the application and database connection are working.

**Response:**
```json
{
  "status": "healthy"
}
```

## Error Handling

The API returns appropriate HTTP status codes:
- `200`: Success
- `400`: Bad request (invalid parameters)
- `500`: Internal server error
- `503`: Service unavailable (database connection issues)

