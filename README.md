# Databricks Reference Data

A collection of Databricks notebooks that create Unity Catalog functions for accessing external reference data APIs. These functions enable SQL-based access to external data sources directly from your Databricks environment.

## Overview

This repository provides ready-to-use Databricks notebooks that set up Unity Catalog (UC) functions for various external APIs. Once deployed, these functions can be called from SQL, Python, or any Databricks-supported language, making external data easily accessible across your data platform.

## Available Integrations

| Integration | Notebook | Functions Created | Description |
|-------------|----------|-------------------|-------------|
| **Tavily** | `notebooks/tavily.py` | `search()`, `extract()` | Web search and content extraction API |
| **Companies House** | `notebooks/companies_house.py` | `search_companies()`, `get_company_profile()`, `get_company_officers()`, `get_filing_history()` | UK company registration data |
| **Yahoo Finance** | `notebooks/yahoo_finance.py` | `get_stock_info()`, `get_stock_history()`, `get_financials()`, `get_recommendations()`, `get_dividends()` | Stock market data and financial information |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Appropriate permissions to create schemas and functions in your target catalog
- API keys for the services that require them:
  - **Tavily**: Get an API key from [tavily.com](https://tavily.com)
  - **Companies House**: Register at [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk/)
  - **Yahoo Finance**: No API key required (uses yfinance library)

## Usage

### 1. Import the Repository

Clone or import this repository into your Databricks workspace using [Databricks Repos](https://docs.databricks.com/repos/index.html).

### 2. Run a Notebook

Each notebook accepts the following widget parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `api_key` | API key for the external service | `your-api-key-here` |
| `catalog` | Target Unity Catalog name | `main` |
| `schema` | Target schema name (default varies by integration) | `tavily` or `companies_house` |

### 3. Use the Functions

Once the notebook has run successfully, you can use the created functions from any SQL context:

```sql
-- Tavily: Search the web
SELECT main.tavily.search(search_query => 'databricks machine learning');

-- Tavily: Extract content from a URL
SELECT main.tavily.extract(urls => 'https://docs.databricks.com');

-- Companies House: Search for companies
SELECT main.companies_house.search_companies(query => 'Databricks');

-- Companies House: Get company profile
SELECT main.companies_house.get_company_profile(company_number => '14307029');

-- Companies House: Get company officers
SELECT main.companies_house.get_company_officers(company_number => '14307029');

-- Companies House: Get filing history
SELECT main.companies_house.get_filing_history(company_number => '14307029');

-- Yahoo Finance: Get stock info
SELECT main.yahoo_finance.get_stock_info(symbol => 'AAPL');

-- Yahoo Finance: Get stock history
SELECT main.yahoo_finance.get_stock_history(symbol => 'MSFT', period => '1mo', interval_val => '1d');

-- Yahoo Finance: Get financials
SELECT main.yahoo_finance.get_financials(symbol => 'GOOGL', statement_type => 'income');

-- Yahoo Finance: Get analyst recommendations
SELECT main.yahoo_finance.get_recommendations(symbol => 'NVDA');

-- Yahoo Finance: Get dividend history
SELECT main.yahoo_finance.get_dividends(symbol => 'JNJ');
```

## Function Reference

### Tavily Functions

#### `search(search_query, ...)`

Search the web using Tavily's AI-powered search API.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `search_query` | STRING | required | The search query |
| `auto_parameters` | BOOLEAN | FALSE | Let Tavily auto-tune parameters |
| `topic` | STRING | "general" | Search topic category |
| `search_depth` | STRING | "basic" | Search depth ("basic" or "advanced") |
| `chunks_per_source` | INT | 3 | Number of chunks per source |
| `max_results` | INT | 5 | Maximum number of results |
| `include_answer` | BOOLEAN | FALSE | Include AI-generated answer |
| `include_raw_content` | BOOLEAN | FALSE | Include raw page content |
| `include_images` | BOOLEAN | FALSE | Include images |

#### `extract(urls, ...)`

Extract content from URLs using Tavily.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `urls` | STRING | required | URL(s) to extract content from |
| `query` | STRING | NULL | Optional query to focus extraction |
| `chunks_per_source` | INT | 3 | Number of chunks per source |
| `extract_depth` | STRING | "basic" | Extraction depth |
| `include_images` | BOOLEAN | FALSE | Include images |
| `format` | STRING | "markdown" | Output format |

### Companies House Functions

#### `search_companies(query, items_per_page, start_index)`

Search for UK companies.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | STRING | required | Search query |
| `items_per_page` | INT | 10 | Results per page |
| `start_index` | INT | 0 | Pagination offset |

#### `get_company_profile(company_number)`

Get detailed company information.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `company_number` | STRING | required | UK company registration number |

#### `get_company_officers(company_number, items_per_page, start_index)`

Get list of company officers/directors.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `company_number` | STRING | required | UK company registration number |
| `items_per_page` | INT | 35 | Results per page |
| `start_index` | INT | 0 | Pagination offset |

#### `get_filing_history(company_number, items_per_page, start_index)`

Get company filing history.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `company_number` | STRING | required | UK company registration number |
| `items_per_page` | INT | 25 | Results per page |
| `start_index` | INT | 0 | Pagination offset |

### Yahoo Finance Functions

#### `get_stock_info(symbol)`

Get comprehensive stock information including company details, market cap, P/E ratio, sector, and more.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `symbol` | STRING | required | Stock ticker symbol (e.g., AAPL, MSFT, GOOGL) |

#### `get_stock_history(symbol, period, interval_val)`

Get historical OHLCV (Open, High, Low, Close, Volume) data.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | STRING | Stock ticker symbol |
| `period` | STRING | Data period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max |
| `interval_val` | STRING | Data interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo |

#### `get_financials(symbol, statement_type)`

Get financial statements for a company.

| Parameter | Type | Description |
|-----------|------|-------------|
| `symbol` | STRING | Stock ticker symbol |
| `statement_type` | STRING | Statement type: "income", "balance", or "cashflow" |

#### `get_recommendations(symbol)`

Get analyst recommendations and ratings.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `symbol` | STRING | required | Stock ticker symbol |

#### `get_dividends(symbol)`

Get dividend payment history.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `symbol` | STRING | required | Stock ticker symbol |

## Architecture

### Authentication Patterns

The notebooks implement two authentication patterns based on API requirements:

1. **Bearer Token (Tavily)**: Uses Databricks HTTP connections with bearer token authentication. The API key is passed as a connection parameter.

2. **Basic Auth (Companies House)**: Uses Python UDFs with an inner/outer function pattern:
   - **Inner functions** (`*_inner`): Python functions that handle Basic auth and API calls
   - **Outer functions**: SQL wrappers that retrieve the API key from Databricks Secrets and pass it to inner functions

3. **No Auth (Yahoo Finance)**: Uses Python UDFs with the yfinance library, which scrapes public Yahoo Finance data. No API key is required.

### Security

- API keys are stored in Databricks Secrets and never exposed in queries
- HTTP connections are created within the target schema
- Functions automatically retrieve credentials at runtime

## Contributing

To add a new integration:

1. Create a new notebook in the `notebooks/` folder
2. Follow the existing patterns for widget parameters and function creation
3. Document the functions in this README
4. Ensure the notebook is self-contained and idempotent (can be run multiple times)

## Development

### Linting and Formatting

This project uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting:

```bash
# Install ruff
pip install ruff

# Check for issues
ruff check notebooks/

# Format code
ruff format notebooks/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Resources

- [Databricks Unity Catalog Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf.html)
- [Databricks HTTP Connections](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-connection.html)
- [Tavily API Documentation](https://docs.tavily.com/)
- [Companies House API Documentation](https://developer.company-information.service.gov.uk/)
- [yfinance Library](https://github.com/ranaroussi/yfinance)
- [Yahoo Finance](https://finance.yahoo.com/)
