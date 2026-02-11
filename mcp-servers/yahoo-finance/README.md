# Yahoo Finance MCP Server

An MCP (Model Context Protocol) server for accessing stock market data using the yfinance library. No API key is required.

## Features

- **get_stock_info**: Get comprehensive stock information
- **get_stock_history**: Get historical OHLCV data
- **get_financials**: Get income statement, balance sheet, or cash flow
- **get_recommendations**: Get analyst recommendations
- **get_dividends**: Get dividend payment history

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (recommended) for dependency management
- No API key required

## Local Development

### Install dependencies

```bash
uv sync
```

### Run the server

```bash
uv run yahoo-finance-mcp-server
```

The server will start at `http://localhost:8000`.

## Deployment to Databricks Apps

### 1. Create the app

```bash
databricks apps create mcp-yahoo-finance --description "MCP Server for Yahoo Finance stock data"
```

### 2. Deploy the app

```bash
DATABRICKS_USERNAME=$(databricks current-user me | jq -r .userName)
databricks sync . "/Users/$DATABRICKS_USERNAME/mcp-yahoo-finance"
databricks apps deploy mcp-yahoo-finance --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/mcp-yahoo-finance"
```

## Available Tools

### get_stock_info

Get comprehensive stock information.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | str | Stock ticker symbol (e.g., "AAPL") |

### get_stock_history

Get historical OHLCV data.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | str | Stock ticker symbol |
| `period` | str | Data period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max |
| `interval` | str | Data interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo |

### get_financials

Get financial statements.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | str | Stock ticker symbol |
| `statement_type` | str | "income", "balance", or "cashflow" |

### get_recommendations

Get analyst recommendations.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | str | Stock ticker symbol |

### get_dividends

Get dividend history.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | str | Stock ticker symbol |

## Testing

```python
from databricks_mcp import DatabricksMCPClient

client = DatabricksMCPClient(server_url="http://localhost:8000/mcp")
tools = client.list_tools()
print(tools)
```

## Notes

- **No API Key Required**: The yfinance library scrapes publicly available Yahoo Finance data
- **Rate Limits**: Yahoo Finance may rate-limit requests; use responsibly
- **Data Accuracy**: Data is sourced from Yahoo Finance and may have slight delays
