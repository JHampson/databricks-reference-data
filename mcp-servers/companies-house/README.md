# Companies House MCP Server

An MCP (Model Context Protocol) server for accessing UK company registration data via the Companies House API.

## Features

- **search_companies**: Search for UK companies by name
- **get_company_profile**: Get detailed company information
- **get_company_officers**: Get list of directors and officers
- **get_filing_history**: Get company filing history

## Prerequisites

- Python 3.11+
- Companies House API key (register at [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk/))
- [uv](https://github.com/astral-sh/uv) (recommended) for dependency management

## Local Development

### Install dependencies

```bash
uv sync
```

### Set environment variables

```bash
export COMPANIES_HOUSE_API_KEY="your-api-key"
```

### Run the server

```bash
uv run companies-house-mcp-server
```

The server will start at `http://localhost:8000`.

## Deployment to Databricks Apps

### 1. Create the app

```bash
databricks apps create mcp-companies-house --description "MCP Server for UK Companies House API"
```

### 2. Store your API key in Databricks Secrets

```bash
databricks secrets create-scope companies_house
databricks secrets put-secret companies_house api_key --string-value "your-api-key"
```

### 3. Add the secret as an app resource

The secret must be added as a resource to the app so it can be injected as an environment variable:

```bash
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

Alternatively, add the secret resource via the Databricks Apps UI under **Configure > App resources > Add resource > Secret**.

### 4. Deploy the app

```bash
DATABRICKS_USERNAME=$(databricks current-user me | jq -r .userName)
databricks sync . "/Users/$DATABRICKS_USERNAME/mcp-companies-house"
databricks apps deploy mcp-companies-house --source-code-path "/Workspace/Users/$DATABRICKS_USERNAME/mcp-companies-house"
```

## Available Tools

### search_companies

Search for UK companies.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | str | required | Search query |
| `items_per_page` | int | 10 | Results per page |
| `start_index` | int | 0 | Pagination offset |

### get_company_profile

Get detailed company information.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `company_number` | str | required | UK company registration number |

### get_company_officers

Get list of company officers.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `company_number` | str | required | UK company registration number |
| `items_per_page` | int | 35 | Results per page |
| `start_index` | int | 0 | Pagination offset |

### get_filing_history

Get company filing history.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `company_number` | str | required | UK company registration number |
| `items_per_page` | int | 25 | Results per page |
| `start_index` | int | 0 | Pagination offset |

## Testing

```python
from databricks_mcp import DatabricksMCPClient

client = DatabricksMCPClient(server_url="http://localhost:8000/mcp")
tools = client.list_tools()
print(tools)
```
