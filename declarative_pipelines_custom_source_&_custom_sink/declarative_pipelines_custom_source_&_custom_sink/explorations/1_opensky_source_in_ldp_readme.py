# Databricks notebook source
# MAGIC %md
# MAGIC # Processing Millions of Events from Thousands of Aircraft with One Declarative Pipeline 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 Custom Source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook defines and registers a custom [PySpark streaming data source](https://docs.databricks.com/aws/en/pyspark/datasources) that polls the [OpenSky Network](https://opensky-network.org/) REST API for live aircraft. Then uses a Databricks Declarative Pipeline table definition to continuously ingest the stream into a managed table.
# MAGIC
# MAGIC Reference architecture and concepts are based on the Databricks blog: [“Processing Millions of Events from Thousands of Aircraft with One Declarative Pipeline”](https://www.databricks.com/blog/processing-millions-events-thousands-aircraft-one-declarative-pipeline).

# COMMAND ----------

# MAGIC %md
# MAGIC ## What it creates
# MAGIC - A Spark streaming data source named opensky-flights: `custom DataSource + StreamReader`
# MAGIC
# MAGIC - A declarative pipeline table opensky_flights created from `spark.readStream.format("opensky-flights").load()`

# COMMAND ----------

# MAGIC %md
# MAGIC ## How it works 
# MAGIC - The stream reader polls the OpenSky REST API on a fixed interval and converts each returned aircraft “state” into a row matching the declared Spark schema.​
# MAGIC
# MAGIC - Basic record validation drops rows missing core fields such as latitude/longitude/altitude/velocity, keeping the downstream table usable for analytics.​
# MAGIC
# MAGIC - The pipeline table function reads from the custom format and produces a continuously updating table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to run
# MAGIC - Attach this notebook to a cluster / pipeline environment that supports running declarative tables from notebook source.​
# MAGIC
# MAGIC - Run the notebook (or configure it as the pipeline source), then start the pipeline so ingestion begins and the opensky_flights table updates continuously.