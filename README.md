# **MetroPulse — Real-Time Traffic Intelligence**


MetroPulse is a real-time traffic intelligence platform that ingests, cleans, and analyzes continuous city sensor data to detect unusual road behavior. It processes live events through a modern streaming pipeline using Spark and Delta Lake, producing reliable and enriched insights. The system identifies anomalies such as sudden speed drops or unexpected congestion through windowed statistical analysis. MetroPulse is built to support fast, data-driven visibility for intelligent urban monitoring.


## Overview

MetroPulse simulates and processes live traffic sensor events using a production-grade streaming architecture. The pipeline captures raw data from Kafka, enriches and structures it through Spark Structured Streaming, and stores it in Delta Lake across Bronze, Silver, and Gold layers. Anomaly detection runs continuously using windowed statistical analysis.

---

## Key Features

- Real-time ingestion of traffic sensor data  
- Kafka-based event streaming  
- Bronze → Silver → Gold Delta Lake architecture  
- Spark Structured Streaming ETL  
- Z-score–based anomaly detection  
- MinIO object storage  
- Docker-based distributed environment  
- Optional Grafana dashboards for visualization  

---

## Tech Stack

- **Apache Kafka** — Real-time event ingestion  
- **Apache Spark** — Streaming processing + anomaly detection  
- **Delta Lake** — ACID data lake storage  
- **MinIO** — S3-compatible object storage  
- **Docker Compose** — Service orchestration  
- **Python** — Producers and ETL jobs  
- **Grafana** — Monitoring & visualization  

---

## Project Structure

<img width="636" height="624" alt="Architecture_FlowChart" src="https://github.com/user-attachments/assets/e81b695f-f810-4a6d-892d-455f72eeed4d" />


---

## How It Works

1. **Producers** publish simulated traffic sensor events into Kafka.  
2. **Bronze Stream** captures raw events from Kafka into Delta tables.  
3. **Silver Stream** cleans and enriches the data for analytical use.  
4. **Gold Stream** computes rolling averages and z-scores to detect anomalies.  
5. Output is written to a Delta table for dashboards, analytics, or ML.  

---

## Running the Project

Start the infrastructure:

```bash
docker compose up -d
```
## Run the producer:
```python spark_jobs/producers.py```

## Start Bronze & Silver ETL:
```python spark_jobs/stream_etl.py```
## Run the anomaly detector:
```python spark_jobs/anomaly_job.py```

# Visualisation:
Grafana:
http://localhost:3000

# Summary 

MetroPulse demonstrates a complete, end-to-end real-time data engineering system using industry-standard tools. 
It showcases streaming ingestion, transformation, storage, and anomaly detection in a modern, scalable architecture.



