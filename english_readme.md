# Project: Tibia Highscore Data Lakehouse

## Overview

This project aims to build a complete Data Lakehouse solution with Apache Airflow orchestration, PySpark distributed processing, MinIO (S3) storage, and Apache Iceberg tables for versioned data management and efficient analytical queries.
The pipeline is designed to collect, transform, and serve data in an automated way, ensuring scalability, governance, and reproducibility across all stages.

## Objective

Extract player ranking data, skills, and other categories from Tibia.
Data is structured by:
  - Vocation: Knight, Paladin, Druid, Sorcerer, Monk, and characters without vocation. 
  - World type: Open PvP, Optional PvP, Hardcore PvP, Retro Open PvP, Retro Hardcore PvP.
  - Skills: Magic Level, Sword, etc.
  - Extras: Achievements, Drome Score, Fishing, etc.
Ensure independent Airflow tasks by vocation and category, enabling parallelism and isolated failures.
Save structured data in the Bronze layer, allowing transformations into Silver and Gold layers.

## General Architecture

The pipeline follows the medallion architecture pattern (Bronze → Silver → Gold), integrating open-source, S3-compatible components.

```text
          +-----------------------------+
          |  Web Scraping (Python)      |
          |  Requests + BeautifulSoup   |
          +-------------+---------------+
                        |
                        v
             +----------+----------+
             |  MinIO (Data Lake)  |
             |  Landing Layer      |
             +----------+----------+
                        |
                        v
             +----------+----------+
             |  MinIO (Data Lake)  |
             |  Bronze Layer       |
             +----------+----------+
                        |
                        v
          +-------------+--------------+
          |  Apache Spark Cluster      |
          |  Silver/Gold Transformations|
          +-------------+--------------+
                        |
                        v
        +---------------+----------------+
        | Apache Iceberg + Nessie Catalog |
        | Versioning and Governance       |
        +---------------+----------------+
                        |
                        v
              +---------+---------+
              |   Dremio Engine   |
              |   SQL on Lake     |
              +---------+---------+
                        |
                        v
    +------------------------------------+
    | Streamlit Dashboards / Grafana     |
    | Visualization and Monitoring       |
    +------------------------------------+
```

## Technology Stack

| Layer | Technologies | Description |
|--------|--------------|------------|
| Collection (Ingestion)  | Python (Requests, BeautifulSoup)	| Web scraping automation for collecting raw data. |
| Storage (Landing) |	MinIO (S3-compatible) |	Data Lake for raw data storage. |
| Processing (Bronze/Silver/Gold)	| Apache Spark Cluster	Distributed data processing and transformation. | 
|  |	Apache Iceberg	| Transactional table format with versioning, schema evolution, and time travel. |
|	 | Nessie Data Catalog	| Git-like version control and data governance. |
| Orchestration	Apache Airflow	| End-to-end pipeline coordination (scraping → transform → load → dashboards). |
| Query & Exploration	| Dremio Query Engine |	SQL engine for querying the Lakehouse (MinIO + Iceberg + Nessie).|
| Testing & Exploration | Jupyter Notebook | Experimentation and transformation validation environment. |
| Visualization & Monitoring	| Streamlit	Interactive dashboards and visual insights.|
|	  | Prometheus + Grafana	| Pipeline performance and system health monitoring.|

## Pipeline Flow

### 1. Data Extraction 
  - Each vocation and category has an independent Airflow task.
  - Vocations: no_vocation, knight, paladin, sorcerer, druid, monk.
  - Extra categories: achievements, fishing, loyalty, drome, boss, charm, goshnair.
  - Skills: axe, sword, club, distance, magic_level, fist, shielding.
  - Task failures don’t affect others, enabling parallelism and traceability.
  - Data is saved to MinIO as CSV (Landing layer).

Example of final DataFrame:

| Rank | Name                | Vocation       | World     | Level | Points         | WorldType |
|------|--------------------|----------------|-----------|-------|----------------|-----------|
| 1    | Khaos Poderoso      | Master Sorcerer | Rasteibra | 2515  | 264,738,322,692 | Open PvP  |
| 2    | Goa Luccas          | Master Sorcerer | Inabra    | 2357  | 217,738,829,108 | Open PvP  |
| 3    | Syriz               | Master Sorcerer | Thyria    | 2189  | 174,396,658,081 | Open PvP  |
| 4    | Dany Ellmagnifico   | Master Sorcerer | Inabra    | 2160  | 167,580,849,914 | Open PvP  |
| 5    | Zonatto Bombinhams  | Master Sorcerer | Honbra    | 2132  | 161,212,779,898 | Open PvP  |



### 2. Bronze Layer
  - Creation of Iceberg tables.
  - Ensures full data auditability and historical tracking.

### 3. Silver Layer
  - UNDER CONSTRUCTION

### 4. Gold Layer
  - UNDER CONSTRUCTION

### 5. Query and Visualization
  - Dremio enables SQL queries over versioned Iceberg tables.
  - Streamlit dashboards display rankings, skills, and historical evolution.
  - Prometheus + Grafana monitor performance and system metrics.

## Orchestration with Apache Airflow
The project uses two main DAGs to manage the complete data flow, ensuring the extraction and processing are organized, scalable, and traceable.

### 1 - Extraction & Ingestion DAG (landing_highscores_pipeline)
Goal: Collect raw Tibia data by vocation, skill, and extra categories, and store it in the Landing layer (MinIO/S3) as partitioned CSV files.
Execution details:
  - Each vocation and category runs as an independent task, enabling parallel execution
  - Task failures do not stop the overall pipeline.
  - After extraction, data is ready for Bronze processing.
    
Layers involved: Landing → Bronze (pre-processing, validation, and organization).
Example tasks:
  - extract_vocation (none, knight, paladin, sorcerer, druid, monk)
  - extract_skills (axe, sword, club, distance, magic_level, fist, shielding)
  - extract_extra (achievements, fishing, loyalty, drome, boss, charm, goshnair)

Output:
```text
s3://landing/year=YYYY/month=MM/day=DD/<category>/<name>.csv
```

### 2 - Lakehouse DAG (lakehouse_pipeline)
Goal: Process Bronze data and generate versioned tables in the Silver and Gold layers using Spark, Iceberg, and Nessie.
Dependency: Triggered automatically once the extraction DAG completes successfully (via Airflow ExternalTaskSensor).
Execution details:
  - Each Bronze category has an independent Spark job:
  - Bronze Vocation → Silver Vocation
  - Bronze Skills → Silver Skills
  - Bronze Extra → Silver Extra

Spark jobs are configured with all necessary JARs (AWS, Iceberg, Nessie) to ensure full S3/MinIO integration.
Layers involved: Bronze → Silver → Gold (transformations, cleaning, aggregations, versioning).

Output:
Versioned and auditable Iceberg tables, ready for SQL querying and dashboards.

```txt
landing_highscores_pipeline (Extraction DAG)
        |
        v
lakehouse_pipeline (Processing DAG)
        |
        v
Bronze -> Silver -> Gold (Iceberg + Nessie)
```

## Project Structure

```txt
├── docker
│   ├── airflow
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── notebook
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── prometheus
│   │   └── prometheus.yaml
│   └── spark
│       └── Dockerfile
├── Makefile
├── mnt
│   ├── airflow
│   │   └── dags
│   │       ├── lakehouse.py
│   │       └── landing.py
│   ├── minio
│   │   ├── bronze
│   │   └── landing
│   │       └── year=2025
│   │           └── month=10
│   │               └── day=23
│   │                   ├── experience
│   │                   ├── extra
│   │                   └── skills
│   ├── notebooks
│   ├── requirements.txt
│   └── src
│       ├── jobs
│       ├── landing
│       ├── patch.txt
│       ├── README.md
│       └── tests
├── README.md
├── services
│   ├── lakehouse.yaml
│   ├── observability.yaml
│   ├── orchestration.yaml
│   ├── processing.yaml
│   └── visualization.yaml
```
IN PROGRESS
