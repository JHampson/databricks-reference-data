# Agent Instructions

This document provides instructions for AI agents working with this repository.

## Repository Purpose

This repository contains Databricks notebooks that create Unity Catalog (UC) functions for accessing external reference data APIs. The functions enable SQL-based access to external data sources from within Databricks.

## Repository Structure

```
databricks-reference-data/
├── notebooks/           # Databricks notebooks that create UC functions
│   ├── tavily.py       # Web search and content extraction
│   ├── companies_house.py  # UK company data
│   └── yahoo_finance.py    # Stock market data
├── pyproject.toml      # Project config and ruff settings
├── README.md           # User documentation
├── AGENTS.md           # This file
└── LICENSE             # MIT License
```

## Available Integrations

### Tavily (`notebooks/tavily.py`)

Creates web search and content extraction functions.

**Required Parameters:**
- `api_key`: Tavily API key
- `catalog`: Target Unity Catalog name
- `schema`: Schema name (default: "tavily")

**Functions Created:**

| Function | Signature | Description |
|----------|-----------|-------------|
| `search` | `search(search_query STRING, ...) RETURNS STRING` | Search the web |
| `extract` | `extract(urls STRING, ...) RETURNS STRING` | Extract content from URLs |

**Example Usage:**
```sql
SELECT catalog.tavily.search(search_query => 'databricks');
SELECT catalog.tavily.extract(urls => 'https://example.com');
```

### Companies House (`notebooks/companies_house.py`)

Creates functions for UK company registration data.

**Required Parameters:**
- `api_key`: Companies House API key
- `catalog`: Target Unity Catalog name
- `schema`: Schema name (default: "companies_house")

**Functions Created:**

| Function | Signature | Description |
|----------|-----------|-------------|
| `search_companies` | `search_companies(query STRING, items_per_page INT DEFAULT 10, start_index INT DEFAULT 0) RETURNS STRING` | Search for companies |
| `get_company_profile` | `get_company_profile(company_number STRING) RETURNS STRING` | Get company details |
| `get_company_officers` | `get_company_officers(company_number STRING, items_per_page INT DEFAULT 35, start_index INT DEFAULT 0) RETURNS STRING` | Get company officers |
| `get_filing_history` | `get_filing_history(company_number STRING, items_per_page INT DEFAULT 25, start_index INT DEFAULT 0) RETURNS STRING` | Get filing history |

**Example Usage:**
```sql
SELECT catalog.companies_house.search_companies(query => 'Databricks');
SELECT catalog.companies_house.get_company_profile(company_number => '14307029');
SELECT catalog.companies_house.get_company_officers(company_number => '14307029');
SELECT catalog.companies_house.get_filing_history(company_number => '14307029');
```

### Yahoo Finance (`notebooks/yahoo_finance.py`)

Creates functions for stock market data and financial information using the yfinance library.

**Required Parameters:**
- `catalog`: Target Unity Catalog name
- `schema`: Schema name (default: "yahoo_finance")

**Note:** No API key required - uses the yfinance Python library which scrapes public Yahoo Finance data.

**Functions Created:**

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_stock_info` | `get_stock_info(symbol STRING) RETURNS STRING` | Get comprehensive stock information |
| `get_stock_history` | `get_stock_history(symbol STRING, period STRING, interval_val STRING) RETURNS STRING` | Get historical OHLCV data |
| `get_financials` | `get_financials(symbol STRING, statement_type STRING) RETURNS STRING` | Get financial statements |
| `get_recommendations` | `get_recommendations(symbol STRING) RETURNS STRING` | Get analyst recommendations |
| `get_dividends` | `get_dividends(symbol STRING) RETURNS STRING` | Get dividend history |

**Example Usage:**
```sql
SELECT catalog.yahoo_finance.get_stock_info(symbol => 'AAPL');
SELECT catalog.yahoo_finance.get_stock_history(symbol => 'MSFT', period => '5d', interval_val => '1d');
SELECT catalog.yahoo_finance.get_financials(symbol => 'GOOGL', statement_type => 'income');
SELECT catalog.yahoo_finance.get_recommendations(symbol => 'NVDA');
SELECT catalog.yahoo_finance.get_dividends(symbol => 'JNJ');
```

## Working with This Repository

### Adding a New Integration

1. Create a new notebook in `notebooks/` following this pattern:
   - Add widget parameters for `api_key`, `catalog`, and `schema`
   - Set catalog and schema context
   - Create HTTP connection or use Python UDFs for API calls
   - Create UC functions with clear COMMENT descriptions
   - Include test cells

2. Update `README.md` with:
   - New row in the integrations table
   - Function reference section
   - Usage examples

3. Update this file with the new integration details

### Code Style

- Use ruff for linting and formatting
- Follow existing notebook patterns
- Use descriptive function COMMENTs for UC functions
- Include default values where sensible

### Authentication Patterns

**Bearer Token APIs (e.g., Tavily):**
- Create HTTP CONNECTION with bearer_token option
- Use `http_request()` in SQL functions

**Basic Auth APIs (e.g., Companies House):**
- Store API key in Databricks Secrets
- Create Python UDFs with inner/outer pattern:
  - Inner function: handles auth and API call
  - Outer function: retrieves secret and calls inner

**No Auth APIs (e.g., Yahoo Finance):**
- Use Python UDFs directly
- Leverage libraries like yfinance that handle data retrieval
- No secrets or connections needed

### Testing

Each notebook includes test cells at the end. Run the entire notebook to:
1. Create all functions
2. Execute test queries
3. Verify results

### Common Tasks

**Run linting:**
```bash
ruff check notebooks/
```

**Format code:**
```bash
ruff format notebooks/
```

**Deploy to Databricks:**
1. Import this repo via Databricks Repos
2. Open the desired notebook
3. Set widget parameters (api_key, catalog, schema)
4. Run all cells

## Return Value Format

All functions return JSON strings. Parse with `from_json()` in SQL:

```sql
SELECT from_json(
    catalog.companies_house.search_companies('Databricks'),
    'STRUCT<items:ARRAY<STRUCT<company_number:STRING, title:STRING>>, total_results:INT>'
) as result;
```

## Error Handling

Functions return the raw API response including error messages. Check the response structure for error fields when results are unexpected.
