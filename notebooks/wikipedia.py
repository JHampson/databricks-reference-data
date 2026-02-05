# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# Create a text widget for catalog name
dbutils.widgets.text("catalog", "", "Catalog Name")
catalog_name: str = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog)

# COMMAND ----------

dbutils.widgets.text("schema", "wikipedia", "Schema")
schema: str = dbutils.widgets.get("schema")
print(f"Using schema: {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the schema
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)
# MAGIC COMMENT 'Schema for wikipedia integration and functions';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA IDENTIFIER(:schema)

# COMMAND ----------

# DBTITLE 1,Create volume for Wikipedia data
# MAGIC %sql
# MAGIC -- Create a volume to store Wikipedia data
# MAGIC CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_data')
# MAGIC COMMENT 'Volume for storing Wikipedia dump files';

# COMMAND ----------

# DBTITLE 1,Install required libraries
# MAGIC %pip install mwxml requests mwparserfromhell

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract Data

# COMMAND ----------

# DBTITLE 1,Download Wikipedia dump to volume
import os
from pathlib import Path

import requests

# Get catalog and schema from widgets
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")

# Use volume path instead of /tmp
volume_path: str = f"/Volumes/{catalog_name}/{schema_name}/wikipedia_data"
local_path: str = f"{volume_path}/wikipedia_dump.xml.bz2"

# Using full English Wikipedia dump
# Note: This is a large file (20GB+ compressed, 80GB+ uncompressed)
wiki_url: str = (
    "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
)

print(f"Volume path: {volume_path}")
print(f"Downloading English Wikipedia dump from {wiki_url}...")
print(
    "WARNING: This is a large file (20GB+ compressed). Download may take 30+ minutes..."
)

# Connection timeout of 30s, no read timeout for large streaming download
response = requests.get(wiki_url, stream=True, timeout=(30, None))
total_size = int(response.headers.get("content-length", 0))

with open(local_path, "wb") as f:
    downloaded = 0
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                percent = (downloaded / total_size) * 100
                print(
                    f"\rProgress: {percent:.1f}% ({downloaded / (1024 * 1024 * 1024):.2f} GB / {total_size / (1024 * 1024 * 1024):.2f} GB)",
                    end="",
                )

print(f"\nDownload complete! File saved to {local_path}")
print(f"File size: {os.path.getsize(local_path) / (1024 * 1024 * 1024):.2f} GB")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw

# COMMAND ----------

# DBTITLE 1,Decompress the bz2 file
import bz2
import shutil

# Get paths from volume
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
volume_path: str = f"/Volumes/{catalog_name}/{schema_name}/wikipedia_data"

compressed_path: str = f"{volume_path}/wikipedia_dump.xml.bz2"
decompressed_path: str = f"{volume_path}/wikipedia_dump.xml"

print("Decompressing bz2 file...")
with bz2.open(compressed_path, "rb") as f_in:
    with open(decompressed_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

print(f"Decompression complete! File saved to {decompressed_path}")
print(f"Decompressed size: {os.path.getsize(decompressed_path) / (1024 * 1024):.2f} MB")

# COMMAND ----------

# DBTITLE 1,Parse XML and write to Delta in batches
from datetime import datetime

import mwxml
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# Get XML path from volume
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
volume_path: str = f"/Volumes/{catalog_name}/{schema_name}/wikipedia_data"
xml_path: str = f"{volume_path}/wikipedia_dump.xml"
json_intermediate_path: str = f"{volume_path}/wikipedia_articles_json"

# Delete intermediate JSON files before starting
dbutils.fs.rm(json_intermediate_path, True)

# Define schema for articles table
articles_schema = StructType(
    [
        StructField("page_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("revision_id", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("contributor_id", LongType(), True),
        StructField("contributor_name", StringType(), True),
        StructField("text", StringType(), True),
        StructField("text_length", IntegerType(), True),
        StructField("comment", StringType(), True),
    ]
)

# Table name
table_name: str = f"{catalog_name}.{schema_name}.wikipedia_articles"

print("Parsing Wikipedia XML dump and writing to JSON in batches...")
print(f"Intermediate JSON path: {json_intermediate_path}\n")

# Batch configuration - reduced to avoid gRPC message size limits
BATCH_SIZE: int = 10000
articles_batch: list = []
count: int = 0
batch_num: int = 0

import json
import os
import uuid

# Ensure intermediate directory exists
os.makedirs(json_intermediate_path, exist_ok=True)


def write_batch_to_json(articles_batch: list, batch_num: int, count: int) -> None:
    json_file = os.path.join(
        json_intermediate_path, f"batch_{batch_num}_{uuid.uuid4().hex}.json"
    )
    with open(json_file, "w", encoding="utf-8") as jf:
        for art in articles_batch:
            jf.write(json.dumps(art, ensure_ascii=False) + "\n")
    print(
        f"Batch {batch_num}: Wrote {len(articles_batch)} articles to JSON (Total: {count})"
    )


with open(xml_path, "rb") as f:
    dump = mwxml.Dump.from_file(f)

    for page in dump:
        # Skip redirect pages and non-article pages
        if page.redirect or page.namespace != 0:
            continue

        # Get the latest revision
        for revision in page:
            article = {
                "page_id": page.id,
                "title": page.title,
                "revision_id": revision.id,
                "timestamp": revision.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                if revision.timestamp
                else None,
                "contributor_id": revision.user.id if revision.user else None,
                "contributor_name": revision.user.text if revision.user else None,
                "text": revision.text if revision.text else "",
                "text_length": len(revision.text) if revision.text else 0,
                "comment": revision.comment if hasattr(revision, "comment") else None,
            }
            articles_batch.append(article)
            count += 1

            # Write batch to JSON when batch size is reached
            if len(articles_batch) >= BATCH_SIZE:
                batch_num += 1
                write_batch_to_json(articles_batch, batch_num, count)
                articles_batch = []  # Clear batch

            # Only take the latest revision
            break

# Write remaining articles in final batch
if articles_batch:
    batch_num += 1
    write_batch_to_json(articles_batch, batch_num, count)

print(f"\n[OK] Parsing complete! Total articles processed: {count}")
print(f"[OK] Data written to JSON files at: {json_intermediate_path}")

# Read JSON files as Spark DataFrame and write to Delta table
print("\nLoading JSON files into Spark DataFrame and writing to Delta table...")
df = spark.read.schema(articles_schema).json(json_intermediate_path)
df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(table_name)
print(f"[OK] Data written to table: {table_name}")

# Clean up intermediate JSON files
import shutil

if os.path.exists(json_intermediate_path):
    shutil.rmtree(json_intermediate_path)
    print(f"[OK] Cleaned up intermediate JSON files at: {json_intermediate_path}")

# COMMAND ----------

# DBTITLE 1,Verify table and show sample data
# Get table name
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
table_name: str = f"{catalog_name}.{schema_name}.wikipedia_articles"

# Read the table
df = spark.table(table_name)

print(f"Table: {table_name}")
print(f"Total rows: {df.count():,}")
print("\nSchema:")
df.printSchema()

print("\nSample data:")
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Verify table and show statistics
# MAGIC %sql
# MAGIC -- Show table statistics
# MAGIC SELECT
# MAGIC     COUNT(*) as total_articles,
# MAGIC     COUNT(DISTINCT contributor_name) as unique_contributors,
# MAGIC     AVG(text_length) as avg_text_length,
# MAGIC     MAX(text_length) as max_text_length,
# MAGIC     MIN(timestamp) as earliest_edit,
# MAGIC     MAX(timestamp) as latest_edit
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_articles')

# COMMAND ----------

# DBTITLE 1,Create wikipedia_articles_latest with CDF enabled
# MAGIC %sql
# MAGIC -- Create the target table with CDF enabled
# MAGIC CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_articles_latest')
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC AS SELECT * FROM IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_articles')
# MAGIC WHERE 1=0  -- Create the table but don't copy data

# COMMAND ----------

# DBTITLE 1,Merge data from wikipedia_articles
# MAGIC %sql
# MAGIC -- Merge data: UPDATE when revision_id differs, INSERT new rows, DELETE rows not in source
# MAGIC MERGE INTO IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_articles_latest') AS target
# MAGIC USING IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_articles') AS source
# MAGIC ON target.page_id = source.page_id
# MAGIC WHEN MATCHED AND target.revision_id <> source.revision_id THEN
# MAGIC   UPDATE SET
# MAGIC     target.title = source.title,
# MAGIC     target.revision_id = source.revision_id,
# MAGIC     target.timestamp = source.timestamp,
# MAGIC     target.contributor_id = source.contributor_id,
# MAGIC     target.contributor_name = source.contributor_name,
# MAGIC     target.text = source.text,
# MAGIC     target.text_length = source.text_length,
# MAGIC     target.comment = source.comment
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (page_id, title, revision_id, timestamp, contributor_id, contributor_name, text, text_length, comment)
# MAGIC   VALUES (source.page_id, source.title, source.revision_id, source.timestamp, source.contributor_id, source.contributor_name, source.text, source.text_length, source.comment)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE

# COMMAND ----------

# DBTITLE 1,Verify the new table
# MAGIC %sql
# MAGIC -- Verify the merge results
# MAGIC SELECT
# MAGIC     COUNT(*) as total_articles,
# MAGIC     COUNT(DISTINCT contributor_name) as unique_contributors,
# MAGIC     AVG(text_length) as avg_text_length,
# MAGIC     MAX(text_length) as max_text_length,
# MAGIC     MIN(timestamp) as earliest_edit,
# MAGIC     MAX(timestamp) as latest_edit
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_articles_latest')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean

# COMMAND ----------

# DBTITLE 1,Define markup removal function
from collections.abc import Iterator

import mwparserfromhell
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


def remove_wikipedia_markup(text):
    """
    Remove Wikipedia markup from article text using mwparserfromhell.
    This library is specifically designed for parsing MediaWiki markup.
    """
    if not text or pd.isna(text):
        return ""

    try:
        # Parse the wikitext
        wikicode = mwparserfromhell.parse(text)

        # Strip all markup and return plain text
        # This removes templates, links, formatting, etc.
        plain_text = wikicode.strip_code()

        # Clean up extra whitespace
        plain_text = " ".join(plain_text.split())

        return plain_text.strip()
    except Exception:
        # If parsing fails, return empty string
        return ""


# Register as Pandas UDF (vectorized) for better performance
# Using iterator of series for optimal memory usage with large datasets
@pandas_udf(StringType())
def remove_markup_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Vectorized UDF that processes data in batches using Pandas.
    This is significantly faster than row-by-row processing.
    """
    for series in iterator:
        # Apply the markup removal function to each element in the batch
        yield series.apply(remove_wikipedia_markup)


print("[OK] Markup removal function defined using mwparserfromhell")
print("[OK] Pandas UDF registered for vectorized processing (faster!)")
print("[OK] Using iterator of series pattern for optimal memory usage")

# COMMAND ----------

# DBTITLE 1,Create wikipedia_articles_cleaned table
# Create the target table if it doesn't exist
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
target_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_cleaned"

# Create empty table with the correct schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    page_id BIGINT,
    title STRING,
    revision_id BIGINT,
    timestamp STRING,
    contributor_id BIGINT,
    contributor_name STRING,
    text_cleaned STRING,
    text_length_original INT,
    text_length_cleaned INT,
    comment STRING
)
USING DELTA
""")

print(f"[OK] Target table created/verified: {target_table}")

# COMMAND ----------

# DBTITLE 1,Stream CDF changes and create cleaned table
from pyspark.sql.functions import col, length, when

# Get parameters
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
source_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_latest"
target_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_cleaned"
checkpoint_path: str = (
    f"/Volumes/{catalog_name}/{schema_name}/wikipedia_data/checkpoints/cdf_stream"
)

print(f"Source table: {source_table}")
print(f"Target table: {target_table}")
print(f"Checkpoint path: {checkpoint_path}")

# Read CDF stream from source table
df_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", "0")  # Start from beginning
    .table(source_table)
)

# Process all change types: insert, update, delete
# For inserts and updates, clean the text
# For deletes, we'll handle them in the merge
df_changes = (
    df_stream.withColumn(
        "text_cleaned",
        when(
            col("_change_type").isin(["insert", "update_postimage"]),
            remove_markup_udf(col("text")),
        ).otherwise(None),
    )
    .withColumn(
        "text_length_cleaned",
        when(
            col("_change_type").isin(["insert", "update_postimage"]),
            length(col("text_cleaned")),
        ).otherwise(None),
    )
    .select(
        col("page_id"),
        col("title"),
        col("revision_id"),
        col("timestamp"),
        col("contributor_id"),
        col("contributor_name"),
        col("text_cleaned"),
        col("text_length").alias("text_length_original"),
        col("text_length_cleaned"),
        col("comment"),
        col("_change_type"),
    )
)


# Define merge function to handle inserts, updates, and deletes
def merge_to_cleaned_table(batch_df, batch_id):
    # Create temp view for the batch
    batch_df.createOrReplaceTempView("cdf_batch")

    # Perform MERGE operation
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING cdf_batch AS source
    ON target.page_id = source.page_id
    WHEN MATCHED AND source._change_type IN ('update_postimage', 'update_preimage') THEN
      UPDATE SET
        target.title = source.title,
        target.revision_id = source.revision_id,
        target.timestamp = source.timestamp,
        target.contributor_id = source.contributor_id,
        target.contributor_name = source.contributor_name,
        target.text_cleaned = source.text_cleaned,
        target.text_length_original = source.text_length_original,
        target.text_length_cleaned = source.text_length_cleaned,
        target.comment = source.comment
    WHEN MATCHED AND source._change_type = 'delete' THEN
      DELETE
    WHEN NOT MATCHED AND source._change_type = 'insert' THEN
      INSERT (page_id, title, revision_id, timestamp, contributor_id, contributor_name,
              text_cleaned, text_length_original, text_length_cleaned, comment)
      VALUES (source.page_id, source.title, source.revision_id, source.timestamp,
              source.contributor_id, source.contributor_name, source.text_cleaned,
              source.text_length_original, source.text_length_cleaned, source.comment)
    """

    spark.sql(merge_sql)
    print(f"Batch {batch_id}: Processed {batch_df.count()} changes")


# Write stream using foreachBatch to enable MERGE operations
query = (
    df_changes.writeStream.foreachBatch(merge_to_cleaned_table)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # Process all available data then stop
    .start()
)

print(f"\n[OK] Streaming query started: {query.name}")
print("Processing CDF changes (inserts, updates, deletes) with Pandas UDF...")

# Wait for the stream to complete
query.awaitTermination()

print("\n[OK] Stream processing complete!")
print(f"[OK] Cleaned articles written to: {target_table}")
print("[OK] Deletes from source are now reflected in cleaned table")

# COMMAND ----------

# DBTITLE 1,Verify cleaned table
# Get table name
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
table_name: str = f"{catalog_name}.{schema_name}.wikipedia_articles_cleaned"

# Read the cleaned table
df_cleaned = spark.table(table_name)

print(f"Table: {table_name}")
print(f"Total rows: {df_cleaned.count():,}")
print("\nSchema:")
df_cleaned.printSchema()

# Show statistics
print("\nStatistics:")
df_stats = df_cleaned.selectExpr(
    "COUNT(*) as total_articles",
    "COUNT(DISTINCT contributor_name) as unique_contributors",
    "AVG(text_length_original) as avg_original_length",
    "AVG(text_length_cleaned) as avg_cleaned_length",
    "AVG(text_length_original - text_length_cleaned) as avg_markup_removed",
    "MIN(timestamp) as earliest_edit",
    "MAX(timestamp) as latest_edit",
)

display(df_stats)

# COMMAND ----------

# DBTITLE 1,Compare original vs cleaned text
from pyspark.sql.functions import col

# Get table names
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
original_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_latest"
cleaned_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_cleaned"

# Join original and cleaned tables to compare
df_original = spark.table(original_table).select(
    col("page_id"),
    col("title"),
    col("text").alias("text_original"),
    col("text_length").alias("length_original"),
)

df_cleaned = spark.table(cleaned_table).select(
    col("page_id"),
    col("text_cleaned"),
    col("text_length_cleaned").alias("length_cleaned"),
)

# Join and show comparison
df_comparison = (
    df_original.join(df_cleaned, "page_id")
    .select(
        col("title"),
        col("length_original"),
        col("length_cleaned"),
        (col("length_original") - col("length_cleaned")).alias("markup_removed"),
        col("text_original").substr(1, 500).alias("original_preview"),
        col("text_cleaned").substr(1, 500).alias("cleaned_preview"),
    )
    .orderBy(col("markup_removed").desc())
)

print("Comparison of original vs cleaned text (top articles by markup removed):")
print("\nShowing first 500 characters of each...\n")
display(df_comparison.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search
# MAGIC
# MAGIC Create overlapping chunks, generate embeddings, and serve in a vector search index.

# COMMAND ----------

# DBTITLE 1,Install required libraries
# MAGIC %pip install databricks-vectorsearch langchain langchain-text-splitters
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Define chunking strategy with LangChain
from collections.abc import Iterator

import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

# Define the Chunking Logic
# Wikipedia articles are often formatted with Markdown or plain text paragraphs.
# We use a chunk size of ~1000 chars (approx 250-300 tokens) for good retrieval context.
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,  # Adjust based on your embedding model's optimal window
    chunk_overlap=100,  # Context overlap prevents cutting sentences in half
    length_function=len,
    separators=[
        "\n\n",
        "\n",
        ". ",
        " ",
        "",
    ],  # Try to split by paragraph first, then sentences
)

# Define schema for the UDF output
chunk_schema = ArrayType(
    StructType(
        [
            StructField("chunk_text", StringType(), True),
            StructField("chunk_index", StringType(), True),
            StructField("chunk_metadata", MapType(StringType(), StringType()), True),
        ]
    )
)


@pandas_udf(chunk_schema)
def chunk_text_udf(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Pandas UDF to chunk text using LangChain's RecursiveCharacterTextSplitter.
    Processes batches of text for better performance.
    """
    for series in batch_iter:
        results = []
        for text in series:
            # Handle nulls or empty text
            if not text or len(str(text).strip()) == 0:
                results.append([])
                continue

            # Split the text using LangChain
            chunks = text_splitter.create_documents([str(text)])

            # Format output as a list of structs
            row_chunks = [
                {
                    "chunk_text": chunk.page_content,
                    "chunk_index": str(idx),
                    "chunk_metadata": {
                        "source": "wikipedia",
                        "chunk_size": str(len(chunk.page_content)),
                        "splitter": "RecursiveCharacterTextSplitter",
                    },
                }
                for idx, chunk in enumerate(chunks)
            ]
            results.append(row_chunks)
        yield pd.Series(results)


print("[OK] Chunking function defined using LangChain RecursiveCharacterTextSplitter")
print("  - Chunk size: 1000 characters (~250-300 tokens)")
print("  - Overlap: 100 characters")
print("  - Separators: paragraph > newline > sentence > space")

# COMMAND ----------

# DBTITLE 1,Show CDF changes processed
from pyspark.sql.functions import col

# Get table name
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
source_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_latest"

print(f"Reading Change Data Feed from: {source_table}\n")

# Read CDF to see what changes were processed
df_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", "0")
    .table(source_table)
)

# Show change type distribution
print("Change type distribution:")
df_change_summary = (
    df_cdf.groupBy(col("_change_type")).count().orderBy("count", ascending=False)
)
display(df_change_summary)

# Show sample of changes by type
print("\nSample changes by type:")
for change_type in ["insert", "update_postimage", "update_preimage", "delete"]:
    df_sample = (
        df_cdf.filter(f"_change_type = '{change_type}'")
        .select(
            "_change_type", "_commit_version", "_commit_timestamp", "page_id", "title"
        )
        .limit(5)
    )

    count = df_cdf.filter(f"_change_type = '{change_type}'").count()
    if count > 0:
        print(f"\n{change_type.upper()} changes (showing 5 of {count:,}):")
        display(df_sample)

# COMMAND ----------

# DBTITLE 1,Create and populate chunks table
from pyspark.sql.functions import col, concat_ws, explode, lit, md5, to_json

# Get parameters
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
source_table: str = f"{catalog_name}.{schema_name}.wikipedia_articles_cleaned"
chunks_table: str = f"{catalog_name}.{schema_name}.wikipedia_chunks"

print(f"Source table: {source_table}")
print(f"Target chunks table: {chunks_table}")
print("\nProcessing articles into chunks...")

# Read cleaned articles
df_articles = (
    spark.table(source_table)
    .select(
        col("page_id"),
        col("title"),
        col("text_cleaned"),
        col("revision_id"),
        col("timestamp"),
    )
    .filter(col("text_cleaned").isNotNull() & (col("text_cleaned") != ""))
)

print(f"Articles to process: {df_articles.count():,}")

# Apply chunking UDF and explode into separate rows
df_chunked = (
    df_articles.withColumn("chunks", chunk_text_udf(col("text_cleaned")))
    .select(
        col("page_id").alias("parent_id"),
        col("title"),
        col("revision_id"),
        col("timestamp"),
        explode(col("chunks")).alias("chunk_data"),
    )
    .select(
        col("parent_id"),
        col("title"),
        col("chunk_data.chunk_text").alias("chunk_text"),
        col("chunk_data.chunk_index").alias("chunk_index"),
        to_json(col("chunk_data.chunk_metadata")).alias(
            "metadata"
        ),  # Convert map to JSON string for vector search compatibility
        col("revision_id"),
        col("timestamp"),
    )
)

# Create a unique ID for every chunk (Critical for Vector Search)
# Using MD5 hash of parent_id + chunk_index for deterministic IDs
df_final = df_chunked.withColumn(
    "chunk_id", md5(concat_ws("_", col("parent_id"), col("chunk_index")))
)

print("\nWriting chunks to Delta table...")

# Write to Delta table
df_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(chunks_table)

chunk_count = spark.table(chunks_table).count()
print(f"\n[OK] Chunks table created: {chunks_table}")
print(f"[OK] Total chunks: {chunk_count:,}")

# Show sample
print("\nSample chunks:")
display(
    spark.table(chunks_table)
    .select(
        "chunk_id",
        "parent_id",
        "title",
        "chunk_index",
        col("chunk_text").substr(1, 100).alias("chunk_preview"),
        "metadata",
    )
    .limit(5)
)

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed on chunks table
# MAGIC %sql
# MAGIC -- Enable CDF for vector search sync
# MAGIC ALTER TABLE IDENTIFIER(:catalog || '.' || :schema || '.wikipedia_chunks')
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC SELECT 'CDF enabled on chunks table' as status

# COMMAND ----------

# DBTITLE 1,Create vector search endpoint
from databricks.vector_search.client import VectorSearchClient

# Initialize client
vsc = VectorSearchClient()

# Endpoint name
endpoint_name: str = "wikipedia_vector_search_endpoint"

print(f"Creating vector search endpoint: {endpoint_name}")

try:
    # Check if endpoint exists
    endpoint = vsc.get_endpoint(endpoint_name)
    current_type = endpoint.get("endpoint_type", "UNKNOWN")
    print(f"[OK] Endpoint already exists: {endpoint_name}")
    print(f"  Current type: {current_type}")

    if current_type != "STORAGE_OPTIMIZED":
        print(f"\n⚠ Endpoint is {current_type}, not STORAGE_OPTIMIZED")
        print(
            "  Note: To change endpoint type, you must delete and recreate the endpoint"
        )
except Exception:
    # Create endpoint if it doesn't exist
    print("Creating new storage-optimized endpoint...")
    vsc.create_endpoint(
        name=endpoint_name,
        endpoint_type="STORAGE_OPTIMIZED",  # Use storage-optimized for cost efficiency with large datasets
    )
    print(f"[OK] Endpoint created: {endpoint_name}")
    print("  Type: STORAGE_OPTIMIZED")
    print("  Note: Endpoint may take a few minutes to become ready")

# Wait for endpoint to be ready
import time

print("\nWaiting for endpoint to be ready...")
while True:
    endpoint = vsc.get_endpoint(endpoint_name)
    status = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")
    print(f"  Status: {status}")
    if status == "ONLINE":
        print("[OK] Endpoint is ready!")
        break
    elif status in ["OFFLINE", "PROVISIONING"]:
        time.sleep(30)
    else:
        print(f"⚠ Unexpected status: {status}")
        break

# COMMAND ----------

# DBTITLE 1,Create delta sync vector index
from databricks.vector_search.client import VectorSearchClient

# Get parameters
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
chunks_table: str = f"{catalog_name}.{schema_name}.wikipedia_chunks"
index_name: str = f"{catalog_name}.{schema_name}.wikipedia_chunks_index"
endpoint_name: str = "wikipedia_vector_search_endpoint"

# Initialize client
vsc = VectorSearchClient()

print(f"Source table: {chunks_table}")
print(f"Index name: {index_name}")
print(f"Endpoint: {endpoint_name}")

try:
    # Check if index exists
    existing_index = vsc.get_index(endpoint_name, index_name)
    print(f"\n⚠ Index already exists: {index_name}")
    print("Deleting existing index to recreate...")
    vsc.delete_index(endpoint_name, index_name)
    print("[OK] Existing index deleted")
    import time

    time.sleep(10)  # Wait for deletion to complete
except Exception:
    print("\nNo existing index found (this is expected for first run)")

print("\nCreating storage-optimized delta sync vector index...")
print("This will:")
print("  1. Automatically generate embeddings using a foundation model")
print("  2. Sync changes from the source table via CDF")
print("  3. Keep the index up-to-date automatically")
print("  4. Use storage-optimized endpoint for cost efficiency")

# Create the index with delta sync
# Note: Storage-optimized endpoints don't support columns_to_sync parameter
# All columns from source table will be synced (metadata is now a string, not map)
index = vsc.create_delta_sync_index(
    endpoint_name=endpoint_name,
    index_name=index_name,
    source_table_name=chunks_table,
    pipeline_type="TRIGGERED",  # Use TRIGGERED for manual sync (required for storage-optimized)
    primary_key="chunk_id",
    embedding_source_column="chunk_text",
    embedding_model_endpoint_name="databricks-gte-large-en",  # Databricks foundation model
)

print(f"\n[OK] Vector index created: {index_name}")
print("[OK] Endpoint type: STORAGE_OPTIMIZED")
print("[OK] Using embedding model: databricks-gte-large-en")
print("[OK] Primary key: chunk_id")
print("[OK] Embedding source: chunk_text")
print("\nIndex will now sync data from the source table...")

# COMMAND ----------

# DBTITLE 1,Monitor index sync status
import time

from databricks.vector_search.client import VectorSearchClient

# Get parameters
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
index_name: str = f"{catalog_name}.{schema_name}.wikipedia_chunks_index"
endpoint_name: str = "wikipedia_vector_search_endpoint"

# Initialize client
vsc = VectorSearchClient(disable_notice=True)

print(f"Monitoring index sync: {index_name}\n")

# Check status periodically
max_wait_minutes: int = 30
check_interval_seconds: int = 30
max_checks: int = (max_wait_minutes * 60) // check_interval_seconds

for i in range(max_checks):
    try:
        index = vsc.get_index(endpoint_name, index_name)
        index_info = index.describe()  # Get dictionary from index object
        status = index_info.get("status", {}).get("detailed_state", "UNKNOWN")
        ready = index_info.get("status", {}).get("ready", False)

        # Get sync status
        index_status = index_info.get("status", {})
        message = index_status.get("message", "")

        print(f"Check {i + 1}/{max_checks}:")
        print(f"  State: {status}")
        print(f"  Ready: {ready}")
        if message:
            print(f"  Message: {message}")

        if ready and status == "ONLINE_INDEXED":
            print("\n[OK] Index is ready and synced!")

            # Show index details
            print("\nIndex details:")
            print(f"  Endpoint: {endpoint_name}")
            print(f"  Index: {index_name}")
            print(f"  Status: {status}")
            break
        elif status in [
            "PROVISIONING",
            "ONLINE_INDEXING",
            "ONLINE_CONTINUOUS_UPDATE",
            "PROVISIONING_INITIAL_SNAPSHOT",
        ]:
            print("  Waiting for sync to complete...\n")
            time.sleep(check_interval_seconds)
        else:
            print(f"\n⚠ Unexpected status: {status}")
            print(f"Full index info: {index_info}")
            break
    except Exception as e:
        print(f"Error checking index status: {e}")
        break
else:
    print(f"\n⚠ Index sync did not complete within {max_wait_minutes} minutes")
    print("You can check the status later or trigger a manual sync")

# COMMAND ----------

# DBTITLE 1,Test vector search
from databricks.vector_search.client import VectorSearchClient

# Get parameters
catalog_name: str = dbutils.widgets.get("catalog")
schema_name: str = dbutils.widgets.get("schema")
index_name: str = f"{catalog_name}.{schema_name}.wikipedia_chunks_index"
endpoint_name: str = "wikipedia_vector_search_endpoint"

# Initialize client
vsc = VectorSearchClient(disable_notice=True)

print(f"Testing vector search on index: {index_name}\n")

# Test query
test_query: str = "What is machine learning and artificial intelligence?"

print(f"Query: '{test_query}'\n")
print("Searching for similar chunks...\n")

# Perform similarity search
results = vsc.get_index(endpoint_name, index_name).similarity_search(
    query_text=test_query,
    columns=["chunk_id", "parent_id", "title", "chunk_text", "chunk_index"],
    num_results=5,
)

print("Top 5 most similar chunks:\n")
print("=" * 80)

for i, result in enumerate(results.get("result", {}).get("data_array", []), 1):
    print(f"\nResult {i}:")
    print(f"  Title: {result[2]}")
    print(f"  Chunk ID: {result[0]}")
    print(f"  Chunk Index: {result[4]}")
    print(f"  Score: {result[-1]:.4f}")  # Last element is typically the score
    print(f"  Text preview: {result[3][:200]}...")
    print("-" * 80)

print("\n[OK] Vector search test complete!")
