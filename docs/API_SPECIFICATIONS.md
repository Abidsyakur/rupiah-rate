# API Specifications

## Data Contracts

### Exchange Rate Record

```json
{
  "date": "2024-01-15",
  "currency_pair": "IDR/USD",
  "opening_rate": 15500.00,
  "closing_rate": 15520.00,
  "highest_rate": 15530.00,
  "lowest_rate": 15490.00,
  "volume": 1000000000,
  "source": "BI",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

### External API Endpoints

#### Fetching Current Rate

```
GET /api/rates/current?pair=IDR/USD
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "rate": 15520.00,
    "timestamp": "2024-01-15T10:00:00Z"
  }
}
```

#### Historical Rates

```
GET /api/rates/historical?start_date=2024-01-01&end_date=2024-01-31
```

**Response:**
```json
{
  "status": "success",
  "data": [
    {
      "date": "2024-01-01",
      "rate": 15500.00
    }
  ]
}
```

## Database Schema

### Staging Tables

`raw_exchange_rates`: Raw data from APIs

### Intermediate Tables

`exchange_rates_daily`: Daily aggregated rates
`exchange_rates_with_ma`: Rates with moving averages

### Mart Tables

`fact_exchange_rates`: Fact table for analytics
`dim_date`: Date dimension
`dim_currency`: Currency dimension

## Error Codes

| Code | Message | Description |
|------|---------|-------------|
| 200 | OK | Success |
| 400 | Bad Request | Invalid parameters |
| 401 | Unauthorized | Authentication required |
| 404 | Not Found | Resource not found |
| 500 | Server Error | Internal error |

## Rate Limiting

- 1000 requests per hour
- 100 requests per minute per IP

## Authentication

Include API key in header:
```
Authorization: Bearer YOUR_API_KEY
```
