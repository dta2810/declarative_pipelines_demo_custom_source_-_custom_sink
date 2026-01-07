# ✈️ Aircraft Flight Data Streaming Pipeline

> Real-time processing of millions of aviation events from thousands of aircraft using Databricks declarative pipelines

[![Databricks](https://img.shields.io/badge/Databricks-DLT-FF3621?logo=databricks)](https://www.databricks.com/)
[![OpenSky Network](https://img.shields.io/badge/Data%20Source-OpenSky%20Network-blue)](https://opensky-network.org/)

## 🎯 Overview

This project demonstrates an end-to-end streaming data pipeline on Databricks that ingests live avionics data from the OpenSky Network API and delivers it to external systems using custom streaming sources and sinks.

## ✨ Features

- 🔄 **Real-time ingestion** of flight state vectors from OpenSky Network API
- 📊 **Managed streaming tables** using Databricks declarative pipelines
- 🚀 **Custom streaming source** implementation with PySpark
- 🌐 **HTTP sink integration** for external system delivery
- 📈 **Scalable processing** of millions of events

## 🏗️ Architecture

OpenSky API → Custom Source → Streaming Table → Append Flow → HTTP Sink → External Endpoint

**Components:**
- Custom Spark streaming source
- Declarative pipeline streaming table: `opensky_flights`
- Append flow with `dlt.read_stream`
- Custom HTTP POST sink (RequestBin/Pipedream)

## 📦 Prerequisites

- ✅ Databricks workspace with Declarative Pipelines support
- ✅ Network access to:
  - [OpenSky Network API](https://opensky-network.org/api/states/all)
  - [RequestBin/Pipedream](https://pipedream.com/requestbin) endpoint
- ✅ Cluster runtime supporting Python Data Source APIs
- ✅ RequestBin or Pipedream account for testing

## 🚀 Getting Started

### Step 1: Navigate to Explorations

- 👉 Open the explorations folder to access the implementation notebooks

### Step 2: Configure Your Environment
- Set up your RequestBin/Pipedream HTTPS endpoint
- Note your endpoint URL for configuration

### Step 3: Run the Pipeline
Notebook 1: Source & Table Setup
- Register the OpenSky custom streaming source
- Create the opensky_flights streaming table
- Start the pipeline

Notebook 2: Sink Configuration
- Register the RequestBin custom sink writer
- Configure sink with your endpoint and batch size
- Start the append flow

### Step 4: Validate
- Monitor the lakehouse table in Databricks
- Check received payloads in your RequestBin/Pipedream dashboard

📝 License
- This project is available for educational and demonstration purposes.

🤝 Contributing
- Contributions, issues, and feature requests are welcome!

## Ready to process millions of flight events? Head to the explorations folder and get started! 🚀
