# Databricks Reference Data

A collection of tools for accessing external reference data in Databricks. This repository provides two deployment options:

1. **Unity Catalog Functions** - SQL-callable functions deployed via notebooks
2. **MCP Servers** - Model Context Protocol servers deployed as Databricks Apps

## Overview

This repository provides ready-to-use integrations for various external APIs. Choose the deployment method that best fits your use case:

- **UC Functions**: Best for SQL-based access and integration with Databricks SQL warehouses
- **MCP Servers**: Best for AI agents and LLM tool-calling scenarios

## Available Integrations

| Integration | UC Functions | MCP Server | Description |
|-------------|--------------|------------|-------------|
| **Tavily** | `notebooks/tavily.py` | [Marketplace](https://docs.tavily.com/integrations/mcp) | Web search and content extraction |
| **Companies House** | `notebooks/companies_house.py` | `mcp-servers/companies-house/` | UK company registration data |
| **Yahoo Finance** | `notebooks/yahoo_finance.py` | `mcp-servers/yahoo-finance/` | Stock market data |
| **Wikipedia** | `notebooks/wikipedia.py` | - | Wikipedia dump processing and Vector Search |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Appropriate permissions to create schemas and functions in your target catalog
- API keys for the services that require them:
  - **Tavily**: Get an API key from [tavily.com](https://tavily.com)
  - **Companies House**: Register at [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk/)
  - **Yahoo Finance**: No API key required (uses yfinance library)
  - **Wikipedia**: No API key required (downloads public Wikipedia dumps)

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

### Wikipedia Data Pipeline

The Wikipedia notebook (`notebooks/wikipedia.py`) is different from the other integrations - instead of creating UC functions, it sets up a complete data pipeline for semantic search over Wikipedia content.

**What it creates:**
- Downloads the full English Wikipedia dump (~20GB compressed)
- Parses XML and extracts article content using `mwxml` and `mwparserfromhell`
- Creates Delta tables with Change Data Feed enabled:
  - `wikipedia_raw` - Raw parsed articles
  - `wikipedia_latest` - Latest version of each article
  - `wikipedia_cleaned` - Cleaned text content
  - `wikipedia_chunks` - Text chunks for embedding
- Sets up a Databricks Vector Search index for semantic search

**Widget Parameters:**

| Parameter | Description | Example |
|-----------|-------------|---------|
| `catalog` | Target Unity Catalog name | `main` |
| `schema` | Schema name (default: "wikipedia") | `wikipedia` |

**Note:** This notebook processes a large dataset and may take several hours to complete.

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

## MCP Servers

MCP (Model Context Protocol) servers provide an alternative deployment method optimized for AI agents and LLM tool-calling. Each server is deployed as a Databricks App.

### Available MCP Servers

| Server | Directory | Tools | API Key Required |
|--------|-----------|-------|------------------|
| **Companies House** | `mcp-servers/companies-house/` | `search_companies`, `get_company_profile`, `get_company_officers`, `get_filing_history` | Yes |
| **Yahoo Finance** | `mcp-servers/yahoo-finance/` | `get_stock_info`, `get_stock_history`, `get_financials`, `get_recommendations`, `get_dividends` | No |

> **Note:** For Tavily, use the official [Tavily MCP server](https://docs.tavily.com/integrations/mcp) available in the marketplace.

### Deploying an MCP Server

1. **Create the app** (app names use `mcp-` prefix):
   ```bash
   databricks apps create mcp-companies-house --description "MCP Server for UK Companies House API"
   ```

2. **Store secrets and add as app resource** (if required):
   ```bash
   # Create secret scope and store API key
   databricks secrets create-scope companies_house
   databricks secrets put-secret companies_house api_key --string-value "your-api-key"
   
   # Add the secret as an app resource
   databricks apps update mcp-companies-house --json '{
     "resources": [{
       "name": "api_key",
       "secret": {
         "scope": "companies_house",
         "key": "api_key",
         "permission": "READ"
       }
     }]
   }'
   ```

3. **Sync and deploy**:
   ```bash
   cd mcp-servers/companies-house
   DATABRICKS_USERNAME=$(databricks current-user me | jq -r .userName)
   databricks sync . "/Users/$DATABRICKS_USERNAME/mcp-companies-house"
   databricks apps deploy mcp-companies-house --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/mcp-companies-house"
   ```

4. **Connect from an AI agent**:
   ```python
   from databricks_mcp import DatabricksMCPClient
   from databricks.sdk import WorkspaceClient

   mcp_client = DatabricksMCPClient(
       server_url="https://<app-url>/mcp",
       workspace_client=WorkspaceClient()
   )
   tools = mcp_client.list_tools()
   ```

### Local Development

Each MCP server can be run locally for development:

```bash
cd mcp-servers/companies-house
export COMPANIES_HOUSE_API_KEY="your-api-key"
uv sync
uv run companies-house-mcp-server
```

The server will be available at `http://localhost:8000/mcp`.

### App Naming Convention

MCP server apps use the `mcp-` prefix:
- `mcp-companies-house`
- `mcp-yahoo-finance`

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

### Databricks
- [Unity Catalog Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf.html)
- [HTTP Connections](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-connection.html)
- [Databricks Apps](https://docs.databricks.com/dev-tools/databricks-apps/index.html)
- [Custom MCP Servers](https://docs.databricks.com/aws/en/generative-ai/mcp/custom-mcp)

### External APIs
- [Tavily API Documentation](https://docs.tavily.com/)
- [Companies House API Documentation](https://developer.company-information.service.gov.uk/)
- [yfinance Library](https://github.com/ranaroussi/yfinance)
- [Yahoo Finance](https://finance.yahoo.com/)

### MCP Protocol
- [Model Context Protocol](https://modelcontextprotocol.io)
- [FastMCP](https://github.com/jlowin/fastmcp)
