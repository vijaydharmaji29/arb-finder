# API Schema Documentation

This document describes the input and output schemas for all API endpoints in the ChoraBackend service.

## Base URL
All endpoints are relative to the base URL of the deployed service.

---

## Endpoints

### 1. POST `/arbitrage-opportunities`

Get top arbitrage opportunities from the market matching table.

#### Request

**Headers:**
```
Content-Type: application/json
```

**Body (JSON, all fields optional):**
```json
{
  "num_markets": 10,        // integer, default: 10, min: 1, max: 100
  "min_arb_price": 0.5,     // float, default: 0.5, must be >= 0
  "max_arb_price": 0.05      // float, default: 0.05, must be >= min_arb_price
}
```

**Request Schema:**
```typescript
interface ArbitrageOpportunitiesRequest {
  num_markets?: number;      // Optional, default: 10, range: 1-100
  min_arb_price?: number;    // Optional, default: 0.5, must be >= 0
  max_arb_price?: number;  // Optional, default: 0.05, must be >= min_arb_price
}
```

**Example Request:**
```json
{
  "num_markets": 20,
  "min_arb_price": 0.3,
  "max_arb_price": 0.8
}
```

**Empty Request:**
The request body can be empty `{}` or omitted entirely. All parameters will use their default values.

#### Success Response (200 OK)

**Response Schema:**
```typescript
interface ArbitrageOpportunitiesResponse {
  success: boolean;          // Always true for success
  count: number;            // Number of opportunities returned
  opportunities: Opportunity[];
}

interface Opportunity {
  // Market matching fields
  id: number;
  arb_price: number | null;
  confidence: number | null;
  
  // Kalshi market fields
  kalshi_market_id: string | null;
  kalshi_market_title: string | null;
  kalshi_market_yes_price: number | null;
  kalshi_market_no_price: number | null;
  kalshi_market_volume: number | null;
  kalshi_market_open_interest: number | null;
  kalshi_market_status: string | null;
  kalshi_market_subtitle: string | null;
  kalshi_market_no_subtitle: string | null;
  kalshi_market_rules_primary: string | null;
  
  // Kalshi event fields
  kalshi_event_id: string | null;
  kalshi_event_title: string | null;
  kalshi_event_category: string | null;
  kalshi_event_sub_category: string | null;
  kalshi_event_close_time: string | null;  // ISO 8601 format datetime
  kalshi_event_status: string | null;
  
  // Polymarket market fields
  polymarket_market_id: string | null;
  polymarket_market_title: string | null;
  polymarket_market_yes_price: number | null;
  polymarket_market_no_price: number | null;
  polymarket_market_liquidity: number | null;
  polymarket_market_volume: number | null;
  polymarket_market_status: string | null;
  polymarket_market_description: string | null;  
  // Polymarket event fields
  polymarket_event_id: string | null;
  polymarket_event_title: string | null;
  polymarket_event_category: string | null;
  polymarket_event_close_time: string | null;  // ISO 8601 format datetime
  polymarket_event_status: string | null;
}
```

**Example Response:**
```json
{
  "success": true,
  "count": 2,
  "opportunities": [
    {
      "id": 1,
      "arb_price": 0.75,
      "confidence": 0.95,
      "kalshi_market_id": "KALSHI-123",
      "kalshi_market_title": "Will it rain tomorrow?",
      "kalshi_market_yes_price": 0.60,
      "kalshi_market_no_price": 0.40,
      "kalshi_market_volume": 10000.0,
      "kalshi_market_open_interest": 5000.0,
      "kalshi_market_status": "open",
      "kalshi_market_subtitle": "Yes",
      "kalshi_market_no_subtitle": "No",
      "kalshi_market_rules_primary": "Market rules...",
      "kalshi_event_id": "EVENT-123",
      "kalshi_event_title": "Weather Event",
      "kalshi_event_category": "Weather",
      "kalshi_event_sub_category": "Daily",
      "kalshi_event_close_time": "2024-01-15T23:59:59",
      "kalshi_event_status": "open",
      "polymarket_market_id": "POLY-456",
      "polymarket_market_title": "Will it rain tomorrow?",
      "polymarket_market_yes_price": 0.65,
      "polymarket_market_no_price": 0.35,
      "polymarket_market_liquidity": 15000.0,
      "polymarket_market_volume": 12000.0,
      "polymarket_market_status": "open",
      "polymarket_market_description": "Market description...",
      "polymarket_event_id": "POLY-EVENT-456",
      "polymarket_event_title": "Weather Event",
      "polymarket_event_category": "Weather",
      "polymarket_event_close_time": "2024-01-15T23:59:59",
      "polymarket_event_status": "open"
    }
  ]
}
```

#### Error Responses

**400 Bad Request - Invalid Parameter:**
```json
{
  "error": "num_markets must be at least 1"
}
```

**400 Bad Request - Invalid Parameter Range:**
```json
{
  "error": "num_markets must be at most 100"
}
```

**400 Bad Request - Invalid Price:**
```json
{
  "error": "min_arb_price must be non-negative"
}
```

**400 Bad Request - Invalid Price Range:**
```json
{
  "error": "max_arb_price must be >= min_arb_price"
}
```

**400 Bad Request - Invalid Parameter Type:**
```json
{
  "error": "Invalid parameter: invalid literal for int() with base 10: 'abc'"
}
```

**500 Internal Server Error - Database Error:**
```json
{
  "error": "Database error: connection failed"
}
```

**500 Internal Server Error - Unexpected Error:**
```json
{
  "error": "Internal server error: unexpected error occurred"
}
```

---

### 2. GET `/health`

Health check endpoint to verify the service and database connectivity.

#### Request

No request body or parameters required.

#### Success Response (200 OK)

**Response Schema:**
```typescript
interface HealthCheckResponse {
  status: "healthy";
}
```

**Example Response:**
```json
{
  "status": "healthy"
}
```

#### Error Response (503 Service Unavailable)

**Response Schema:**
```typescript
interface HealthCheckErrorResponse {
  status: "unhealthy";
  message: string;
}
```

**Example Response:**
```json
{
  "status": "unhealthy",
  "message": "Database pool not initialized"
}
```

or

```json
{
  "status": "unhealthy",
  "message": "connection timeout"
}
```

---

## Common Error Response Format

All error responses follow this structure:

```typescript
interface ErrorResponse {
  error: string;  // Human-readable error message
}
```

**HTTP Status Codes:**
- `200` - Success
- `400` - Bad Request (invalid parameters)
- `500` - Internal Server Error
- `503` - Service Unavailable (health check only)

---

## Notes for Frontend Team

1. **Content-Type Header**: Always set `Content-Type: application/json` for POST requests, even if the body is empty.

2. **Optional Request Body**: The `/arbitrage-opportunities` endpoint accepts an empty JSON object `{}` or can omit the body entirely. All parameters are optional and will use defaults if not provided.

3. **Date/Time Format**: All datetime fields are returned in ISO 8601 format (e.g., `"2024-01-15T23:59:59"`).

4. **Null Values**: Many fields can be `null` if the data is not available or if there's no matching record in the joined tables.

5. **Price Values**: All price, volume, liquidity, and open_interest fields are returned as floats (numbers with decimals).

6. **Ordering**: Opportunities are returned ordered by `arb_price` in descending order (highest arbitrage price first).

7. **Pagination**: Currently, pagination is handled via the `num_markets` parameter. The endpoint returns up to `num_markets` results.

