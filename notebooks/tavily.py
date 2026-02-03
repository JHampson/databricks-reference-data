# Databricks notebook source
# DBTITLE 1,Create API key parameter widget
# Create a text widget for API key
dbutils.widgets.text("api_key", "", "API Key")
api_key: str = dbutils.widgets.get("api_key")
print(f"Using api_key: {api_key[:8]}..." if len(api_key) > 8 else "API key set")

# COMMAND ----------

# DBTITLE 1,Create catalog parameter widget
# Create a text widget for catalog name
dbutils.widgets.text("catalog", "", "Catalog Name")
catalog_name: str = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Set the catalog context
# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog)

# COMMAND ----------

dbutils.widgets.text("schema", "tavily", "Schema")
schema: str = dbutils.widgets.get("schema")
print(f"Using schema: {schema}")

# COMMAND ----------

# DBTITLE 1,Create tavily schema
# MAGIC %sql
# MAGIC -- Create the schema
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)
# MAGIC COMMENT 'Schema for Tavily integration and functions';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA IDENTIFIER(:schema)

# COMMAND ----------

# DBTITLE 1,Create HTTP Connection to Tavily API
# MAGIC %sql
# MAGIC -- Drop existing HTTP Connection if it exists
# MAGIC DROP CONNECTION IF EXISTS tavily_rest_api;
# MAGIC
# MAGIC -- Create HTTP Connection
# MAGIC CREATE CONNECTION tavily_rest_api
# MAGIC TYPE HTTP
# MAGIC OPTIONS (
# MAGIC   host 'https://api.tavily.com',
# MAGIC   port '443',
# MAGIC   bearer_token :api_key
# MAGIC )
# MAGIC COMMENT 'HTTP connection to Tavily API';

# COMMAND ----------

# DBTITLE 1,Create function to search the web
# MAGIC %sql
# MAGIC -- Create UC Function to search the web using Tavily
# MAGIC CREATE OR REPLACE FUNCTION search(
# MAGIC   search_query STRING,
# MAGIC   auto_parameters BOOLEAN DEFAULT FALSE,
# MAGIC   topic STRING DEFAULT "general",
# MAGIC   search_depth STRING DEFAULT  "basic",
# MAGIC   chunks_per_source INT DEFAULT 3,
# MAGIC   max_results INT DEFAULT 5,
# MAGIC   include_answer BOOLEAN DEFAULT false,
# MAGIC   include_raw_content BOOLEAN DEFAULT false,
# MAGIC   include_images BOOLEAN DEFAULT false,
# MAGIC   include_image_descriptions BOOLEAN DEFAULT false,
# MAGIC   include_favicon BOOLEAN DEFAULT false
# MAGIC   -- include_domains": [],
# MAGIC   -- exclude_domains": [],
# MAGIC   -- country": null,
# MAGIC   -- include_usage": false
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Search the internet using Tavily'
# MAGIC RETURN (
# MAGIC   SELECT http_request(
# MAGIC     conn => 'tavily_rest_api',
# MAGIC     method => 'POST',
# MAGIC     path => '/search',
# MAGIC     json => to_json(named_struct(
# MAGIC       'query', search_query,
# MAGIC       'auto_parameters', auto_parameters,
# MAGIC       'topic', topic,
# MAGIC       'search_depth', search_depth,
# MAGIC       'chunks_per_source', chunks_per_source,
# MAGIC       'max_results', max_results,
# MAGIC       'include_answer', include_answer,
# MAGIC       'include_raw_content', include_raw_content,
# MAGIC       'include_images', include_images,
# MAGIC       'include_image_descriptions', include_image_descriptions,
# MAGIC       'include_favicon', include_favicon
# MAGIC       -- 'include_domains', [],
# MAGIC       -- 'exclude_domains', [],
# MAGIC       -- 'country', null,
# MAGIC       -- 'include_usage', false
# MAGIC     ))
# MAGIC   ).text
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Test the search function
result_df = spark.sql(
    f"SELECT {catalog_name}.{schema}.search(search_query=>'databricks')"
)
display(result_df)

# COMMAND ----------

# DBTITLE 1,Create function to extract content from URLs
# MAGIC %sql
# MAGIC -- Create UC Function to extract content from URLs using Tavily
# MAGIC CREATE OR REPLACE FUNCTION extract(
# MAGIC   urls STRING,
# MAGIC   query STRING DEFAULT NULL,
# MAGIC   chunks_per_source INT DEFAULT 3,
# MAGIC   extract_depth STRING DEFAULT "basic",
# MAGIC   include_images BOOLEAN DEFAULT false,
# MAGIC   include_favicon BOOLEAN DEFAULT false,
# MAGIC   format STRING DEFAULT "markdown",
# MAGIC   timeout STRING DEFAULT null,
# MAGIC   include_usage BOOLEAN DEFAULT false
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Extract content from URLs using Tavily'
# MAGIC RETURN (
# MAGIC   SELECT http_request(
# MAGIC     conn => 'tavily_rest_api',
# MAGIC     method => 'POST',
# MAGIC     path => '/extract',
# MAGIC     json => to_json(named_struct(
# MAGIC       'urls', urls,
# MAGIC       'query', query,
# MAGIC       'chunks_per_source', chunks_per_source,
# MAGIC       'extract_depth', extract_depth,
# MAGIC       'include_images', include_images,
# MAGIC       'include_favicon', include_favicon,
# MAGIC       'format', format,
# MAGIC       'timeout', timeout,
# MAGIC       'include_usage', include_usage
# MAGIC     ))
# MAGIC   ).text
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Test the extract function
# Test the extract function with the Wikipedia AI article
result_df = spark.sql(f"""
  SELECT {catalog_name}.{schema}.extract(
    urls => 'https://en.wikipedia.org/wiki/Artificial_intelligence'
  )
""")
display(result_df)
