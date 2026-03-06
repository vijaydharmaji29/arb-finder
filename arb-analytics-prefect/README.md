# Arbitrage Detection Flow

This Prefect flow detects arbitrage opportunities between matched Kalshi and Polymarket markets.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set the database URL environment variable:
```bash
export DATABASE_URL="postgresql://user:password@host/database"
```

Or create a `.env` file:
```
DATABASE_URL=postgresql://user:password@host/database
```

## Usage

### Run directly:
```bash
python arbitrage_detection.py
```

### Run with Prefect CLI:
```bash
prefect deployment build arbitrage_detection.py:detect_arbitrage_opportunities -n arbitrage-detection
prefect deployment apply detect_arbitrage_opportunities-deployment.yaml
prefect deployment run detect_arbitrage_opportunities/arbitrage-detection
```

## How it works

1. **Fetches matched markets**: Retrieves all records from `market_matching` table where both `kalshi_market_id` and `polymarket_market_id` are present.

2. **Gets market prices**: Fetches current `yes_price` and `no_price` for both Kalshi and Polymarket markets.

3. **Calculates arbitrage**: For each matched pair, checks if:
   - YES prices differ: If Kalshi yes < Polymarket yes, buy on Kalshi and sell on Polymarket (or vice versa)
   - NO prices differ: If Kalshi no < Polymarket no, buy on Kalshi and sell on Polymarket (or vice versa)
   
   The maximum arbitrage opportunity (highest price difference) is calculated.

4. **Updates database**: Updates the `arb_price` field in `market_matching` table with the calculated arbitrage price.

## Arbitrage Types

The flow detects four types of arbitrage:
- `yes_kalshi_buy`: Buy YES on Kalshi (lower price), sell on Polymarket (higher price)
- `yes_polymarket_buy`: Buy YES on Polymarket (lower price), sell on Kalshi (higher price)
- `no_kalshi_buy`: Buy NO on Kalshi (lower price), sell on Polymarket (higher price)
- `no_polymarket_buy`: Buy NO on Polymarket (lower price), sell on Kalshi (higher price)

