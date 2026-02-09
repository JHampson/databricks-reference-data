# Databricks notebook source
# MAGIC %md
# MAGIC # Yahoo Finance Integration
# MAGIC
# MAGIC This notebook creates Unity Catalog Functions for accessing Yahoo Finance data using the `yfinance` Python library.
# MAGIC No API key is required - yfinance scrapes public Yahoo Finance data.

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

# DBTITLE 1,Create schema parameter widget
dbutils.widgets.text("schema", "yahoo_finance", "Schema")
schema: str = dbutils.widgets.get("schema")
print(f"Using schema: {schema}")

# COMMAND ----------

# DBTITLE 1,Create yahoo_finance schema
# MAGIC %sql
# MAGIC -- Create the yahoo_finance schema
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)
# MAGIC COMMENT 'Schema for Yahoo Finance integration and functions';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA IDENTIFIER(:schema)

# COMMAND ----------

# DBTITLE 1,Create function to get stock info
# MAGIC %sql
# MAGIC -- Create UC Function to get stock information
# MAGIC CREATE OR REPLACE FUNCTION get_stock_info(
# MAGIC   symbol STRING COMMENT 'Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC PACKAGES = ('yfinance')
# MAGIC COMMENT 'Get comprehensive stock information from Yahoo Finance including company details, market cap, P/E ratio, sector, and more'
# MAGIC AS $$
# MAGIC import yfinance as yf
# MAGIC import json
# MAGIC
# MAGIC try:
# MAGIC     ticker = yf.Ticker(symbol)
# MAGIC     info = ticker.info
# MAGIC
# MAGIC     if not info or info.get("regularMarketPrice") is None:
# MAGIC         return json.dumps({"error": f"No data found for symbol: {symbol}"})
# MAGIC
# MAGIC     return json.dumps(info)
# MAGIC except Exception as e:
# MAGIC     return json.dumps({"error": "Request failed", "message": str(e)})
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Test get_stock_info
# Test the get_stock_info function
result_df = spark.sql("SELECT get_stock_info('AAPL') AS stock_info")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Create function to get stock history
# MAGIC %sql
# MAGIC -- Create UC Function to get historical stock data
# MAGIC CREATE OR REPLACE FUNCTION get_stock_history(
# MAGIC   symbol STRING COMMENT 'Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)',
# MAGIC   period STRING DEFAULT '1mo' COMMENT 'Data period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max',
# MAGIC   interval_val STRING DEFAULT '1d' COMMENT 'Data interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC PACKAGES = ('yfinance')
# MAGIC COMMENT 'Get historical OHLCV (Open, High, Low, Close, Volume) data for a stock'
# MAGIC AS $$
# MAGIC import yfinance as yf
# MAGIC import json
# MAGIC
# MAGIC try:
# MAGIC     ticker = yf.Ticker(symbol)
# MAGIC     hist = ticker.history(period=period, interval=interval_val)
# MAGIC
# MAGIC     if hist.empty:
# MAGIC         return json.dumps({"error": f"No historical data found for symbol: {symbol}"})
# MAGIC
# MAGIC     # Reset index to include date as a column and convert to JSON-serializable format
# MAGIC     hist = hist.reset_index()
# MAGIC     hist["Date"] = hist["Date"].astype(str)
# MAGIC
# MAGIC     # Convert to list of records
# MAGIC     records = hist.to_dict(orient="records")
# MAGIC
# MAGIC     return json.dumps({
# MAGIC         "symbol": symbol,
# MAGIC         "period": period,
# MAGIC         "interval": interval_val,
# MAGIC         "data": records
# MAGIC     })
# MAGIC except Exception as e:
# MAGIC     return json.dumps({"error": "Request failed", "message": str(e)})
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Test get_stock_history
# Test the get_stock_history function
result_df = spark.sql("SELECT get_stock_history('MSFT', '5d', '1d') AS history")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Create function to get financials
# MAGIC %sql
# MAGIC -- Create UC Function to get financial statements
# MAGIC CREATE OR REPLACE FUNCTION get_financials(
# MAGIC   symbol STRING COMMENT 'Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)',
# MAGIC   statement_type STRING DEFAULT 'income' COMMENT 'Statement type: income, balance, cashflow'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC PACKAGES = ('yfinance')
# MAGIC COMMENT 'Get financial statements (income statement, balance sheet, or cash flow) for a company'
# MAGIC AS $$
# MAGIC import yfinance as yf
# MAGIC import json
# MAGIC
# MAGIC try:
# MAGIC     ticker = yf.Ticker(symbol)
# MAGIC
# MAGIC     if statement_type == "income":
# MAGIC         df = ticker.financials
# MAGIC     elif statement_type == "balance":
# MAGIC         df = ticker.balance_sheet
# MAGIC     elif statement_type == "cashflow":
# MAGIC         df = ticker.cashflow
# MAGIC     else:
# MAGIC         return json.dumps({"error": f"Invalid statement_type: {statement_type}. Use 'income', 'balance', or 'cashflow'"})
# MAGIC
# MAGIC     if df is None or df.empty:
# MAGIC         return json.dumps({"error": f"No financial data found for symbol: {symbol}"})
# MAGIC
# MAGIC     # Convert column names (dates) to strings and transpose for better readability
# MAGIC     df.columns = df.columns.astype(str)
# MAGIC     result = df.to_dict()
# MAGIC
# MAGIC     return json.dumps({
# MAGIC         "symbol": symbol,
# MAGIC         "statement_type": statement_type,
# MAGIC         "data": result
# MAGIC     })
# MAGIC except Exception as e:
# MAGIC     return json.dumps({"error": "Request failed", "message": str(e)})
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Test get_financials
# Test the get_financials function with different statement types
print("Testing income statement...")
result_df = spark.sql("SELECT get_financials('GOOGL', 'income') AS financials")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Create function to get recommendations
# MAGIC %sql
# MAGIC -- Create UC Function to get analyst recommendations
# MAGIC CREATE OR REPLACE FUNCTION get_recommendations(
# MAGIC   symbol STRING COMMENT 'Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC PACKAGES = ('yfinance')
# MAGIC COMMENT 'Get analyst recommendations and ratings for a stock'
# MAGIC AS $$
# MAGIC import yfinance as yf
# MAGIC import json
# MAGIC
# MAGIC try:
# MAGIC     ticker = yf.Ticker(symbol)
# MAGIC     recommendations = ticker.recommendations
# MAGIC
# MAGIC     if recommendations is None or recommendations.empty:
# MAGIC         return json.dumps({"error": f"No recommendations found for symbol: {symbol}"})
# MAGIC
# MAGIC     # Reset index and convert to JSON-serializable format
# MAGIC     recommendations = recommendations.reset_index()
# MAGIC     if "Date" in recommendations.columns:
# MAGIC         recommendations["Date"] = recommendations["Date"].astype(str)
# MAGIC
# MAGIC     records = recommendations.to_dict(orient="records")
# MAGIC
# MAGIC     return json.dumps({
# MAGIC         "symbol": symbol,
# MAGIC         "recommendations": records
# MAGIC     })
# MAGIC except Exception as e:
# MAGIC     return json.dumps({"error": "Request failed", "message": str(e)})
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Test get_recommendations
# Test the get_recommendations function
result_df = spark.sql("SELECT get_recommendations('NVDA') AS recommendations")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Create function to get dividends
# MAGIC %sql
# MAGIC -- Create UC Function to get dividend history
# MAGIC CREATE OR REPLACE FUNCTION get_dividends(
# MAGIC   symbol STRING COMMENT 'Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC PACKAGES = ('yfinance')
# MAGIC COMMENT 'Get dividend payment history for a stock'
# MAGIC AS $$
# MAGIC import yfinance as yf
# MAGIC import json
# MAGIC
# MAGIC try:
# MAGIC     ticker = yf.Ticker(symbol)
# MAGIC     dividends = ticker.dividends
# MAGIC
# MAGIC     if dividends is None or dividends.empty:
# MAGIC         return json.dumps({"error": f"No dividend data found for symbol: {symbol}"})
# MAGIC
# MAGIC     # Convert to list of records with date and dividend amount
# MAGIC     records = [
# MAGIC         {"date": str(date), "dividend": float(value)}
# MAGIC         for date, value in dividends.items()
# MAGIC     ]
# MAGIC
# MAGIC     return json.dumps({
# MAGIC         "symbol": symbol,
# MAGIC         "dividends": records
# MAGIC     })
# MAGIC except Exception as e:
# MAGIC     return json.dumps({"error": "Request failed", "message": str(e)})
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,Test get_dividends
# Test the get_dividends function
result_df = spark.sql("SELECT get_dividends('JNJ') AS dividends")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Test all functions with SQL
# MAGIC %sql
# MAGIC -- Test all Yahoo Finance UC functions
# MAGIC
# MAGIC -- 1. Get stock info
# MAGIC SELECT 'Stock Info' AS test_name, get_stock_info('TSLA') AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2. Get stock history
# MAGIC SELECT 'Stock History' AS test_name, get_stock_history('TSLA', '5d', '1d') AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3. Get financials
# MAGIC SELECT 'Financials' AS test_name, get_financials('TSLA', 'income') AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 4. Get recommendations
# MAGIC SELECT 'Recommendations' AS test_name, get_recommendations('TSLA') AS result
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 5. Get dividends
# MAGIC SELECT 'Dividends' AS test_name, get_dividends('TSLA') AS result;

# COMMAND ----------

# DBTITLE 1,Parse JSON example
# MAGIC %sql
# MAGIC -- Example: Parse stock info JSON into structured columns
# MAGIC SELECT
# MAGIC   json_data.shortName AS company_name,
# MAGIC   json_data.sector AS sector,
# MAGIC   json_data.industry AS industry,
# MAGIC   json_data.marketCap AS market_cap,
# MAGIC   json_data.currentPrice AS current_price,
# MAGIC   json_data.trailingPE AS pe_ratio,
# MAGIC   json_data.dividendYield AS dividend_yield,
# MAGIC   json_data.fiftyTwoWeekHigh AS high_52w,
# MAGIC   json_data.fiftyTwoWeekLow AS low_52w
# MAGIC FROM (
# MAGIC   SELECT from_json(
# MAGIC     get_stock_info('AAPL'),
# MAGIC     'STRUCT<shortName:STRING, sector:STRING, industry:STRING, marketCap:BIGINT, currentPrice:DOUBLE, trailingPE:DOUBLE, dividendYield:DOUBLE, fiftyTwoWeekHigh:DOUBLE, fiftyTwoWeekLow:DOUBLE>'
# MAGIC   ) AS json_data
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Yahoo Finance Integration Setup Complete
# MAGIC
# MAGIC The following components have been created:
# MAGIC
# MAGIC ### Schema
# MAGIC * `${catalog}.yahoo_finance` - Schema for all Yahoo Finance functions
# MAGIC
# MAGIC ### UC Functions
# MAGIC
# MAGIC All functions use the `yfinance` Python library to fetch data from Yahoo Finance. No API key is required.
# MAGIC
# MAGIC #### Available Functions:
# MAGIC
# MAGIC 1. **get_stock_info(symbol)** - Get comprehensive stock information
# MAGIC    * `symbol` (STRING) - Stock ticker symbol (e.g., AAPL, MSFT)
# MAGIC    * Returns: JSON with company details, market cap, P/E ratio, sector, industry, etc.
# MAGIC
# MAGIC 2. **get_stock_history(symbol, period, interval_val)** - Get historical OHLCV data
# MAGIC    * `symbol` (STRING) - Stock ticker symbol
# MAGIC    * `period` (STRING, default: '1mo') - Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
# MAGIC    * `interval_val` (STRING, default: '1d') - Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
# MAGIC    * Returns: JSON with historical Open, High, Low, Close, Volume data
# MAGIC
# MAGIC 3. **get_financials(symbol, statement_type)** - Get financial statements
# MAGIC    * `symbol` (STRING) - Stock ticker symbol
# MAGIC    * `statement_type` (STRING, default: 'income') - Statement type: 'income', 'balance', or 'cashflow'
# MAGIC    * Returns: JSON with financial statement data
# MAGIC
# MAGIC 4. **get_recommendations(symbol)** - Get analyst recommendations
# MAGIC    * `symbol` (STRING) - Stock ticker symbol
# MAGIC    * Returns: JSON with analyst ratings and recommendations
# MAGIC
# MAGIC 5. **get_dividends(symbol)** - Get dividend history
# MAGIC    * `symbol` (STRING) - Stock ticker symbol
# MAGIC    * Returns: JSON with dividend payment history
# MAGIC
# MAGIC ### Usage Examples
# MAGIC
# MAGIC ```sql
# MAGIC -- Get basic stock info
# MAGIC SELECT get_stock_info('AAPL');
# MAGIC
# MAGIC -- Get last 5 days of stock history
# MAGIC SELECT get_stock_history('MSFT', '5d', '1d');
# MAGIC
# MAGIC -- Get income statement
# MAGIC SELECT get_financials('GOOGL', 'income');
# MAGIC
# MAGIC -- Get balance sheet
# MAGIC SELECT get_financials('GOOGL', 'balance');
# MAGIC
# MAGIC -- Get cash flow statement
# MAGIC SELECT get_financials('GOOGL', 'cashflow');
# MAGIC
# MAGIC -- Get analyst recommendations
# MAGIC SELECT get_recommendations('NVDA');
# MAGIC
# MAGIC -- Get dividend history
# MAGIC SELECT get_dividends('JNJ');
# MAGIC
# MAGIC -- Parse JSON into structured data
# MAGIC SELECT
# MAGIC   json_data.shortName AS company_name,
# MAGIC   json_data.marketCap AS market_cap,
# MAGIC   json_data.currentPrice AS price
# MAGIC FROM (
# MAGIC   SELECT from_json(
# MAGIC     get_stock_info('AAPL'),
# MAGIC     'STRUCT<shortName:STRING, marketCap:BIGINT, currentPrice:DOUBLE>'
# MAGIC   ) AS json_data
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Notes
# MAGIC
# MAGIC * **No API Key Required**: The `yfinance` library scrapes publicly available Yahoo Finance data
# MAGIC * **Rate Limits**: Yahoo Finance may rate-limit requests; use responsibly
# MAGIC * **Data Accuracy**: Data is sourced from Yahoo Finance and may have slight delays
# MAGIC * **Market Hours**: Real-time prices are only available during market hours
# MAGIC
# MAGIC ### API Documentation
# MAGIC * yfinance library: https://github.com/ranaroussi/yfinance
# MAGIC * Yahoo Finance: https://finance.yahoo.com/
