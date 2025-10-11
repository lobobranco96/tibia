# Projeto: Tibia Highscore Data Lakehouse

## Visão Geral

Este projeto tem como objetivo construir um Data Lakehouse completo, com orquestração via Apache Airflow, processamento distribuído em PySpark, armazenamento no MinIO (S3) e tabelas Iceberg para gerenciamento de dados versionados e consultas analíticas eficientes.

A pipeline foi projetada para coletar, transformar e disponibilizar dados de forma automatizada, garantindo escalabilidade, governança e reprodutibilidade em todas as etapas.


## Arquitetura Geral

A pipeline segue o padrão **medallion architecture** (Bronze → Silver → Gold), com integração entre componentes open source e compatíveis com S3.

```text
          +-----------------------------+
          |  Web Scraping (Python)      |
          |  Selenium + BeautifulSoup   |
          +-------------+---------------+
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
          |  Transformações Silver/Gold|
          +-------------+--------------+
                        |
                        v
        +---------------+----------------+
        | Apache Iceberg + Nessie Catalog |
        | Versionamento e Governança     |
        +---------------+----------------+
                        |
                        v
              +---------+---------+
              |   Dremio Engine   |
              |   SQL sobre Lake  |
              +---------+---------+
                        |
                        v
    +------------------------------------+
    | Streamlit Dashboards / Grafana     |
    | Visualização e Monitoramento       |
    +------------------------------------+
```

## Stack Tecnológica

| Camada | Tecnologias | Descrição |
|--------|--------------|------------|
| **Coleta (Ingestão)** | **Python (Selenium, BeautifulSoup)** | Automação e raspagem de dados web para coleta de informações brutas. |
| **Armazenamento (Raw/Bronze)** | **MinIO (S3-compatible)** | Data Lake para armazenamento dos dados brutos, intermediários e processados. |
| **Processamento (Silver/Gold)** | **Apache Spark Cluster** | Processamento distribuído e transformação dos dados. |
|  | **Apache Iceberg** | Formato de tabela transacional com versionamento, schema evolution e time travel. |
|  | **Nessie Data Catalog** | Controle de versões e governança dos dados (Git para tabelas). |
| **Orquestração** | **Apache Airflow** | Coordena o pipeline de ponta a ponta (scraping → transformação → carga → dashboards). |
| **Consulta e Exploração** | **Dremio Query Engine** | SQL Engine para consultas sobre o Lakehouse (MinIO + Iceberg + Nessie). |
| **Testes e Exploração Local** | **Jupyter Notebook** | Ambiente de experimentação e validação de transformações. |
| **Visualização e Monitoramento** | **Streamlit** | Dashboards interativos e análises visuais. |
|  | **Prometheus + Grafana** | Monitoramento e observabilidade de métricas (Spark, Airflow, containers, etc). |

## 
