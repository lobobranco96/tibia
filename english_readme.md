## Overview

This project aims to build a complete Data Lakehouse, orchestrated with Apache Airflow, distributed processing using Apache Spark with PySpark, storage on MinIO (S3-compatible), and Apache Iceberg tables for versioned data management and efficient analytical queries.

The pipeline is designed to collect, transform, and expose data in an automated way, ensuring scalability, governance, and reproducibility across all stages.

## Motivation

Tibia has public rankings that are constantly updated, but the data is not available in a structured, historical, or analytical format.
This makes it difficult to perform analyses such as:

  - Player evolution over time
  - Comparison between vocations and world types
  - Historical ranking analysis by skill or experience
  - Creation of customized and reusable dashboards

This project was created to solve this problem through a modern, reliable, and scalable data architecture, enabling historical and versioned analysis of Tibia rankings.

## Objectives

Extract ranking data for players, skills, and other Tibia categories
Organize by:
  - Vocation:
    - Knight, Paladin, Druid, Sorcerer, Monk, and characters without vocation
  - World type:
    - Open PvP, Optional PvP, Hardcore PvP, Retro Open PvP, Retro Hardcore PvP
  - Skills:
    - Magic Level, Sword, etc.
  
Guarantee independent tasks per vocation and category in Airflow, allowing parallelism and isolated failures
Store data in a structured Bronze layer, enabling transformations into Silver and Gold

## Architectural Decision

The architecture follows the Medallion pattern (Landing → Bronze → Silver → Gold) and integrates only open-source technologies, ensuring portability and freedom from vendor lock-in.

Even with small daily ingestions, the Bronze layer was built on Apache Iceberg to guarantee history, versioning, and consistency over time.
Spark is used not because of current data volume, but because it is the most mature engine for transactional writes to Iceberg, integration with Project Nessie, and future pipeline evolution.

This approach prepares the Lakehouse for continuous growth, auditing, and temporal analysis without structural refactoring.

## Summary

  - Overview
  - Source code
  - Lakehouse Data
  - Notebooks
  - Airflow DAGs
  - Docker Services
  - Images
  - Dockerfile Builds
  - Streamlit App – Visualization Layer
  - 
- [Visão Geral](https://github.com/lobobranco96/tibia)
- [Source code](https://github.com/lobobranco96/tibia/tree/main/mnt/src)
- [Lakehouse Data](https://github.com/lobobranco96/tibia/tree/main/mnt/minio/lakehouse)
- [Notebooks](https://github.com/lobobranco96/tibia/tree/main/mnt/notebooks)
- [DAGs Airflow](https://github.com/lobobranco96/tibia/tree/main/mnt/airflow/dags)
- [Docker Services](https://github.com/lobobranco96/tibia/tree/main/services)
- [Images](https://github.com/lobobranco96/tibia/tree/main/docs)
- [Dockerfile Builds](https://github.com/lobobranco96/tibia/tree/main/docker)
- [Streamlit App - Visualization Layer](https://github.com/lobobranco96/tibia/tree/main/docker/streamlit)

## Documentation
- Arquitetura: docs/architecture.md  
- Pipeline: docs/pipeline.md  
- Ambiente: docs/environment.md  
- Visualização: docs/visualization.md
- Tabelas Gold: docs/gold_tables.md
- Extraction: docs/extraction.md




Final Considerations
This project demonstrates the practical application of a modern Lakehouse architecture, focused on versioned data, governance, automation, and analytical consumption, serving as a foundation for historical analysis and advanced Tibia dashboards.
