# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Note: Companies House uses Basic authentication rather than bearer authentication. As a workaround, the API token is being stored as a separate secret so that it can be referenced in the HTTP request.

# COMMAND ----------

# DBTITLE 1,Fetch the API key values
# Create a text widget for API key
dbutils.widgets.text("api_key", "", "API Key")
api_key = dbutils.widgets.get("api_key")
print(f"Using api_key: {api_key}")

# COMMAND ----------

import requests

workspace_url = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)
url = f"{workspace_url}/api/2.0/secrets/scopes/create"
pat = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
data = {"scope": "companies_house"}
response = requests.post(url, headers=headers, json=data)

if response.status_code == 400 and "RESOURCE_ALREADY_EXISTS" in response.text:
    print("Scope already exists, continue")
elif response.status_code != 200:
    raise Exception(f"Failed to create scope: {response.status_code} {response.text}")
else:
    print(response.status_code, response.text)

# COMMAND ----------

import requests

workspace_url = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)
pat = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
scope_name = "companies_house"
key_name = "api_key"
api_key = dbutils.widgets.get("api_key")

url = f"{workspace_url}/api/2.0/secrets/put"
headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
data = {"scope": scope_name, "key": key_name, "string_value": api_key}
response = requests.post(url, headers=headers, json=data)

if response.status_code != 200:
    raise Exception(f"Failed to store secret: {response.status_code} {response.text}")
else:
    print("Secret stored successfully.")

# COMMAND ----------

# DBTITLE 1,Create catalog parameter widget
# Create a text widget for catalog name
dbutils.widgets.text("catalog", "", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Set the schema context
# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog)

# COMMAND ----------

dbutils.widgets.text("schema", "companies_house", "Schema")
schema = dbutils.widgets.get("schema")
print(f"Using schema: {schema}")

# COMMAND ----------

# DBTITLE 1,Create companies_house schema
# MAGIC %sql
# MAGIC -- Create the companies_house schema
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)
# MAGIC COMMENT 'Schema for Companies House API integration and functions';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA IDENTIFIER(:schema)

# COMMAND ----------

# DBTITLE 1,Create SQL helper function to get API key
# MAGIC %sql
# MAGIC -- Create a SQL helper function that returns the API key from secrets
# MAGIC CREATE OR REPLACE FUNCTION get_api_key()
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Helper function to retrieve Companies House API key from secrets'
# MAGIC RETURN secret('companies_house', 'api_key');

# COMMAND ----------

# DBTITLE 1,Create Python function for searching companies
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION search_companies_inner(
# MAGIC   search_query STRING,
# MAGIC   api_key STRING,
# MAGIC   items_per_page INT ,
# MAGIC   start_index INT
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Search for companies using Companies House API'
# MAGIC AS $$
# MAGIC import requests
# MAGIC import base64
# MAGIC
# MAGIC # Construct Basic auth header
# MAGIC auth_header = "Basic " + base64.b64encode((api_key + ":").encode("utf-8")).decode("utf-8")
# MAGIC
# MAGIC # Make API request
# MAGIC url = "https://api.company-information.service.gov.uk/search/companies"
# MAGIC params = {
# MAGIC     "q": search_query,
# MAGIC     "items_per_page": str(items_per_page),
# MAGIC     "start_index": str(start_index)
# MAGIC }
# MAGIC headers = {
# MAGIC     "Authorization": auth_header
# MAGIC }
# MAGIC response = requests.get(url, params=params, headers=headers)
# MAGIC return response.text
# MAGIC $$;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create outer function for searching companies
query = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema}.search_companies(
    query STRING COMMENT 'Search query',
    items_per_page INT DEFAULT 10 COMMENT 'Number of items per page (Default: 10)',
    start_index INT DEFAULT 0 COMMENT 'Start index (Default: 0)'
)
RETURNS STRING
COMMENT 'Search companies using Companies House API'
RETURN
    SELECT {catalog_name}.{schema}.search_companies_inner(
        query,
        {catalog_name}.{schema}.get_api_key(),
        items_per_page,
        start_index
    );
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Test search companies
# MAGIC %sql
# MAGIC -- Test search_companies function and parse JSON response
# MAGIC SELECT
# MAGIC   from_json(
# MAGIC     search_companies('Databricks'),
# MAGIC     'STRUCT<items:ARRAY<STRUCT<company_number:STRING, title:STRING, company_status:STRING, date_of_creation:STRING, address_snippet:STRING>>, total_results:INT>'
# MAGIC   ) as result

# COMMAND ----------

# DBTITLE 1,Create inner function to get company profile
# MAGIC %sql
# MAGIC -- Create inner Python function to get company profile
# MAGIC CREATE OR REPLACE FUNCTION get_company_profile_inner(
# MAGIC   company_number STRING,
# MAGIC   api_key STRING
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Inner function to get detailed company profile from Companies House API'
# MAGIC AS $$
# MAGIC import requests
# MAGIC import base64
# MAGIC
# MAGIC # Construct Basic auth header
# MAGIC auth_header = "Basic " + base64.b64encode((api_key + ":").encode("utf-8")).decode("utf-8")
# MAGIC
# MAGIC # Make API request
# MAGIC url = f"https://api.company-information.service.gov.uk/company/{company_number}"
# MAGIC headers = {
# MAGIC     "Authorization": auth_header
# MAGIC }
# MAGIC response = requests.get(url, headers=headers)
# MAGIC return response.text
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Create outer function to get company profile
query = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema}.get_company_profile(
    company_number STRING COMMENT 'Company number'
)
RETURNS STRING
COMMENT 'Get detailed company profile from Companies House API'
RETURN
    {catalog_name}.{schema}.get_company_profile_inner(
        company_number,
        {catalog_name}.{schema}.get_api_key()
    );
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Test Company Profile
result_df = spark.sql(
    f"SELECT {catalog_name}.{schema}.get_company_profile('14307029') AS company_profile"
)
display(result_df)

# COMMAND ----------

# DBTITLE 1,Create inner function to get company officers
# MAGIC %sql
# MAGIC -- Create inner Python function to get company officers
# MAGIC CREATE OR REPLACE FUNCTION get_company_officers_inner(
# MAGIC   company_number STRING,
# MAGIC   api_key STRING,
# MAGIC   items_per_page INT,
# MAGIC   start_index INT
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Inner function to get list of company officers from Companies House API'
# MAGIC AS $$
# MAGIC import requests
# MAGIC import base64
# MAGIC
# MAGIC # Construct Basic auth header
# MAGIC auth_header = "Basic " + base64.b64encode((api_key + ":").encode("utf-8")).decode("utf-8")
# MAGIC
# MAGIC # Make API request
# MAGIC url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"
# MAGIC params = {
# MAGIC     "items_per_page": str(items_per_page),
# MAGIC     "start_index": str(start_index)
# MAGIC }
# MAGIC headers = {
# MAGIC     "Authorization": auth_header
# MAGIC }
# MAGIC response = requests.get(url, params=params, headers=headers)
# MAGIC return response.text
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Create outer function to get company officers
query = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema}.get_company_officers(
    company_number STRING COMMENT 'Company number',
    items_per_page INT DEFAULT 35 COMMENT 'Number of items per page (Default: 35)',
    start_index INT DEFAULT 0 COMMENT 'Start index (Default: 0)'
)
RETURNS STRING
COMMENT 'Get list of company officers from Companies House API'
RETURN
    {catalog_name}.{schema}.get_company_officers_inner(
        company_number,
        {catalog_name}.{schema}.get_api_key(),
        items_per_page,
        start_index
    );
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Create inner function to get filing history
# MAGIC %sql
# MAGIC -- Create inner Python function to get filing history
# MAGIC CREATE OR REPLACE FUNCTION get_filing_history_inner(
# MAGIC   company_number STRING,
# MAGIC   api_key STRING,
# MAGIC   items_per_page INT,
# MAGIC   start_index INT
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Inner function to get company filing history from Companies House API'
# MAGIC AS $$
# MAGIC import requests
# MAGIC import base64
# MAGIC
# MAGIC # Construct Basic auth header
# MAGIC auth_header = "Basic " + base64.b64encode((api_key + ":").encode("utf-8")).decode("utf-8")
# MAGIC
# MAGIC # Make API request
# MAGIC url = f"https://api.company-information.service.gov.uk/company/{company_number}/filing-history"
# MAGIC params = {
# MAGIC     "items_per_page": str(items_per_page),
# MAGIC     "start_index": str(start_index)
# MAGIC }
# MAGIC headers = {
# MAGIC     "Authorization": auth_header
# MAGIC }
# MAGIC response = requests.get(url, params=params, headers=headers)
# MAGIC return response.text
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Create outer function to get filing history
query = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema}.get_filing_history(
    company_number STRING COMMENT 'Company number',
    items_per_page INT DEFAULT 25 COMMENT 'Number of items per page (Default: 25)',
    start_index INT DEFAULT 0 COMMENT 'Start index (Default: 0)'
)
RETURNS STRING
COMMENT 'Get company filing history from Companies House API'
RETURN
    {catalog_name}.{schema}.get_filing_history_inner(
        company_number,
        {catalog_name}.{schema}.get_api_key(),
        items_per_page,
        start_index
    );
"""

spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Test get_company_officers and get_filing_history
# Test get_company_officers function
print("Testing get_company_officers...")
officers_df = spark.sql(
    f"SELECT {catalog_name}.{schema}.get_company_officers('14307029') AS officers"
)
display(officers_df)

print("\nTesting get_filing_history...")
filing_df = spark.sql(
    f"SELECT {catalog_name}.{schema}.get_filing_history('14307029') AS filing_history"
)
display(filing_df)

# COMMAND ----------

# DBTITLE 1,Test all UC functions with SQL
# MAGIC %sql
# MAGIC -- Test all Companies House UC functions
# MAGIC
# MAGIC -- 1. Search for companies
# MAGIC SELECT 'Search Results' AS test_name, search_companies('Databricks', 5, 0) AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2. Get company profile
# MAGIC SELECT 'Company Profile' AS test_name, get_company_profile('14307029') AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3. Get company officers
# MAGIC SELECT 'Company Officers' AS test_name, get_company_officers('14307029', 10, 0) AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 4. Get filing history
# MAGIC SELECT 'Filing History' AS test_name, get_filing_history('14307029', 10, 0) AS result;

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Companies House API Integration Setup Complete
# MAGIC
# MAGIC The following components have been created:
# MAGIC
# MAGIC ### Schema
# MAGIC * `${catalog}.companies_house` - Schema for all Companies House functions
# MAGIC
# MAGIC ### Secret Management
# MAGIC * API key stored securely in Databricks Secrets (scope: `companies_house`, key: `api_key`)
# MAGIC * `get_api_key()` - SQL helper function to retrieve the API key from secrets
# MAGIC
# MAGIC ### UC Functions
# MAGIC
# MAGIC All functions follow an inner/outer pattern:
# MAGIC * **Inner functions** (`*_inner`) - Python functions that handle API calls with Basic authentication (http_request only supports connections with bearer token)
# MAGIC * **Outer functions** - SQL wrappers that automatically retrieve the API key and pass it to inner functions. SQL functions can also handle default values whereas Python functions cannot
# MAGIC
# MAGIC #### Available Functions:
# MAGIC 1. **search_companies(query, items_per_page, start_index)** - Search for companies
# MAGIC    * `query` (STRING) - Search query
# MAGIC    * `items_per_page` (INT, default: 10) - Number of results per page
# MAGIC    * `start_index` (INT, default: 0) - Starting index for pagination
# MAGIC
# MAGIC 2. **get_company_profile(company_number)** - Get detailed company information
# MAGIC    * `company_number` (STRING) - Company registration number
# MAGIC
# MAGIC 3. **get_company_officers(company_number, items_per_page, start_index)** - Get company officers/directors
# MAGIC    * `company_number` (STRING) - Company registration number
# MAGIC    * `items_per_page` (INT, default: 35) - Number of results per page
# MAGIC    * `start_index` (INT, default: 0) - Starting index for pagination
# MAGIC
# MAGIC 4. **get_filing_history(company_number, items_per_page, start_index)** - Get company filing history
# MAGIC    * `company_number` (STRING) - Company registration number
# MAGIC    * `items_per_page` (INT, default: 25) - Number of results per page
# MAGIC    * `start_index` (INT, default: 0) - Starting index for pagination
# MAGIC
# MAGIC ### Usage Examples
# MAGIC
# MAGIC ```sql
# MAGIC -- Search for companies (returns JSON string)
# MAGIC SELECT search_companies('Databricks');
# MAGIC
# MAGIC -- Search with custom pagination
# MAGIC SELECT search_companies('Databricks', 20, 0);
# MAGIC
# MAGIC -- Parse search results into structured data
# MAGIC SELECT
# MAGIC   from_json(
# MAGIC     search_companies('Databricks'),
# MAGIC     'STRUCT<items:ARRAY<STRUCT<company_number:STRING, title:STRING, company_status:STRING, date_of_creation:STRING, address_snippet:STRING>>, total_results:INT>'
# MAGIC   ) as result;
# MAGIC
# MAGIC -- Get company profile
# MAGIC SELECT get_company_profile('14307029');
# MAGIC
# MAGIC -- Get company officers
# MAGIC SELECT get_company_officers('14307029');
# MAGIC
# MAGIC -- Get filing history
# MAGIC SELECT get_filing_history('14307029');
# MAGIC ```
# MAGIC
# MAGIC ### Architecture Benefits
# MAGIC
# MAGIC * **Security**: API key is never exposed in queries - automatically retrieved from Databricks Secrets
# MAGIC * **Simplicity**: Users call simple outer functions without managing credentials
# MAGIC * **Flexibility**: Inner functions can be called directly if custom authentication is needed
# MAGIC * **Basic Auth Support**: Python functions support Basic authentication (required by Companies House API)
# MAGIC
# MAGIC ### API Documentation
# MAGIC Companies House API documentation: https://developer.company-information.service.gov.uk/
