# Databricks notebook source
# MAGIC %md
# MAGIC # Processing Millions of Events from Thousands of Aircraft with One Declarative Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 Custom Sink

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC This notebook defines a custom streaming sink using the[ PySpark Data Source Writer API](https://docs.databricks.com/aws/en/pyspark/datasources) and connects it to a Lakeflow/Delta Live Tables append flow. 
# MAGIC
# MAGIC The flow reads from the opensky_flights streaming table and sends batched JSON payloads to an HTTP endpoint [(RequestBin/Pipedream](https://pipedream.com/requestbin)) for inspection.
# MAGIC
# MAGIC Reference architecture and concepts are based on the Databricks blog: [“Introducing the Declarative Pipelines Sink API: Write Pipelines to Kafka and External Delta Tables”](https://www.databricks.com/blog/introducing-dlt-sink-api-write-pipelines-kafka-and-external-delta-tables#section-8).

# COMMAND ----------

# MAGIC %md
# MAGIC ## What it creates
# MAGIC - A custom sink format named `requestbin_sink` that can accept the OpenSky schema.
# MAGIC
# MAGIC - A registered sink instance `requestbin_opensky_sink` configured with your RequestBin URL.
# MAGIC
# MAGIC - An append flow `flow_to_requestbin` that streams rows from `opensky_flights` into the sink.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How it works 
# MAGIC - The custom writer batches rows per partition and POSTs them as JSON to your endpoint.
# MAGIC
# MAGIC - Each microbatch calls write(...) on executors to send data and returns a commit message to the driver.
# MAGIC
# MAGIC - The driver’s commit(...) aggregates partition metrics and logs success per batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to run
# MAGIC - Attach the notebook to a cluster or pipeline environment that can run Declarative Pipelines flows from notebooks.
# MAGIC
# MAGIC - Ensure opensky_flights is available and actively updating (or replace with any streaming table using the same schema).
# MAGIC  
# MAGIC - Run the cells to register the sink, create it, and start the append flow.

# COMMAND ----------

spark.sql("USE CATALOG `main_david_thomas`")
spark.sql("USE SCHEMA `dp_demo_sch`")

# COMMAND ----------

display(spark.sql("SELECT * FROM samples.wanderbricks.users"))