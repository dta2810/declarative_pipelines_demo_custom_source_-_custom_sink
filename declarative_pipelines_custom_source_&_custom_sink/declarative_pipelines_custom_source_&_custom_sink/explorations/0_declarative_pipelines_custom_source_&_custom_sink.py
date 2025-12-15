# Databricks notebook source
# MAGIC %md
# MAGIC # Processing Millions of Events from Thousands of Aircraft with One Declarative Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project overview
# MAGIC
# MAGIC This mini-project demonstrates an end-to-end streaming pattern on Databricks:
# MAGIC
# MAGIC 1. Ingest live avionics/flight “state vectors” from the OpenSky Network API into a managed streaming table using a custom PySpark streaming data source + declarative pipeline table definition.
# MAGIC
# MAGIC 2. Deliver that streaming data to an external system using the declarative pipeline Sink API pattern, implemented here as an HTTP POST sink to a RequestBin/Pipedream endpoint.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture at a glance
# MAGIC
# MAGIC - Custom Spark streaming source
# MAGIC - Declarative pipeline streaming table: opensky_flights
# MAGIC - Append flow reads dlt.read_stream
# MAGIC - Declarative pipeline sink 
# MAGIC - External HTTP endpoint 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with support for declarative pipelines / DLT-style notebook definitions.
# MAGIC
# MAGIC - Network access to:
# MAGIC   - https://opensky-network.org/api/states/all
# MAGIC   - A [RequestBin/Pipedream](https://pipedream.com/requestbin) HTTPS endpoint
# MAGIC
# MAGIC - Cluster/runtime that supports PySpark custom data sources (Python Data Source APIs).
# MAGIC
# MAGIC - The pipeline table function reads from the custom format and produces a continuously updating table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to run
# MAGIC - Run Notebook 1 to:
# MAGIC
# MAGIC     - Register the OpenSky custom streaming source.
# MAGIC     - Create/refresh the `opensky_flights` streaming table definition.
# MAGIC     - Start the pipeline (or execute in the supported pipeline notebook mode) so `opensky_flights` is actively being updated.
# MAGIC
# MAGIC - Run Notebook 2 to:
# MAGIC     - Register the `RequestBin` custom sink writer.
# MAGIC     - Create the sink with d`lt.create_sink(...)` (configure your endpoint + batch size).
# MAGIC     - Start the append flow that streams opensky_flights into the sink.
# MAGIC
# MAGIC - Validate results by checking the lakehouse table and the received payloads in your RequestBin/Pipedream dashboard.