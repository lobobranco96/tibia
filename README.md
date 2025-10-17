# Projeto: Tibia Highscore Data Lakehouse

## Visão Geral

Este projeto tem como objetivo construir um Data Lakehouse completo, com orquestração via Apache Airflow, processamento distribuído em PySpark, armazenamento no MinIO (S3) e tabelas Iceberg para gerenciamento de dados versionados e consultas analíticas eficientes.

A pipeline foi projetada para coletar, transformar e disponibilizar dados de forma automatizada, garantindo escalabilidade, governança e reprodutibilidade em todas as etapas.

## Objetivo

Extrair dados de rankings de jogadores, skills e outras categorias do Tibia.
Estruturar por:
 - Vocação: Knight, Paladin, Druid, Sorcerer, Monk e personagens sem vocação.
 - Tipo de mundo: Open PvP, Optional PvP, Hardcore PvP, Retro Open PvP, Retro Hardcore PvP.
 - Skills: Magic Level, Sword e etc
 - Extra: Achievements, Drome Score, Fishing e etc.
 - Garantir tasks independentes por vocação e categoria no Airflow, permitindo paralelismo e falhas isoladas.
 - Salvar dados de forma estruturada na camada Bronze, permitindo transformações em Silver e Gold.


## Arquitetura Geral

A pipeline segue o padrão **medallion architecture** (Bronze → Silver → Gold), com integração entre componentes open source e compatíveis com S3.

```text
          +-----------------------------+
          |  Web Scraping (Python)      |
          |  Requests + BeautifulSoup   |
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
| **Coleta (Ingestão)** | **Python (Requests, BeautifulSoup)** | Automação e raspagem de dados web para coleta de informações brutas. |
| **Armazenamento (Bronze)** | **MinIO (S3-compatible)** | Data Lake para armazenamento dos dados brutos |
| **Processamento (Silver/Gold)** | **Apache Spark Cluster** | Processamento distribuído e transformação dos dados. |
|  | **Apache Iceberg** | Formato de tabela transacional com versionamento, schema evolution e time travel. |
|  | **Nessie Data Catalog** | Controle de versões e governança dos dados (Git para tabelas). |
| **Orquestração** | **Apache Airflow** | Coordena o pipeline de ponta a ponta (scraping → transformação → carga → dashboards). |
| **Consulta e Exploração** | **Dremio Query Engine** | SQL Engine para consultas sobre o Lakehouse (MinIO + Iceberg + Nessie). |
| **Testes e Exploração Local** | **Jupyter Notebook** | Ambiente de experimentação e validação de transformações. |
| **Visualização e Monitoramento** | **Streamlit** | Dashboards interativos e análises visuais. |
|  | **Prometheus + Grafana** | Monitoramento e observabilidade de métricas (Spark, Airflow, containers, etc). |

## Fluxo do Pipeline

### 1. Extração de Dados:
  - Cada vocação e categoria possui uma task independente no Airflow.
  - Vocações: no_vocation, knight, paladin, sorcerer, druid, monk.
  - Categorias extras: achievements, fishing, loyalty, drome, boss, charm, goshnair.
  - Skills: axe, sword, club, distance, magic_level, fist, shielding.
  - Falhas em uma task não afetam as demais, permitindo paralelismo e rastreabilidade.
  - Fluxo Interno:
    - Gerar URLs de scraping para todas as páginas e tipos de mundo.
    - Extrair dados via Requests/BeautifulSoup.
    - Transformar os dados em DataFrame.
    - Concatenar dados por mundo e página.
    - Salvar em MinIO via CSVBronze (camada Bronze).

Exemplo de DataFrame final:

| Rank | Name                | Vocation       | World     | Level | Points         | WorldType |
|------|--------------------|----------------|-----------|-------|----------------|-----------|
| 1    | Khaos Poderoso      | Master Sorcerer | Rasteibra | 2515  | 264,738,322,692 | Open PvP  |
| 2    | Goa Luccas          | Master Sorcerer | Inabra    | 2357  | 217,738,829,108 | Open PvP  |
| 3    | Syriz               | Master Sorcerer | Thyria    | 2189  | 174,396,658,081 | Open PvP  |
| 4    | Dany Ellmagnifico   | Master Sorcerer | Inabra    | 2160  | 167,580,849,914 | Open PvP  |
| 5    | Zonatto Bombinhams  | Master Sorcerer | Honbra    | 2132  | 161,212,779,898 | Open PvP  |


### 2. Camada Bronze:
  - Armazena os dados brutos extraídos, particionados por year/month/day e por categoria.
  - Garantia de auditabilidade e histórico completo de dados.

### 3. Camada Silver:
  - EM CONSTRUCAO

### 4. Camada Gold:
  - EM CONSTRUCAO

### 5. Consulta e Visualização:
  - Dremio permite consultas SQL sobre as tabelas Iceberg versionadas.
  - Streamlit dashboards exibem rankings, skills e evolução histórica.
  - Prometheus + Grafana monitoram performance e saúde do pipeline.

---
## Estrutura do projeto

```bash
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
│   │       └── highscore_pipeline.py
│   ├── minio
│   │   └── bronze
│   │       └── year=2025
│   │           └── month=10
│   │               └── day=16
│   │                   ├── experience
│   │                   │   ├── druid.csv
│   │                   │   ├── knight.csv
│   │                   │   ├── monk.csv
│   │                   │   ├── no_vocation.csv
│   │                   │   ├── paladin.csv
│   │                   │   └── sorcerer.csv
│   │                   ├── extra
│   │                   │   ├── achievements.csv
│   │                   │   ├── boss.csv
│   │                   │   ├── charm.csv
│   │                   │   ├── drome.csv
│   │                   │   ├── fishing.csv
│   │                   │   ├── goshnair.csv
│   │                   │   └── loyalty.csv
│   │                   └── skills
│   │                       ├── axe.csv
│   │                       ├── club.csv
│   │                       ├── distance.csv
│   │                       ├── shielding.csv
│   │                       └── sword.csv
│   ├── notebooks
│   ├── requirements.txt
│   └── src
│       ├── bronze
│       │   ├── bronze_app.py
│       │   ├── extract.py
│       │   ├── __init__.py
│       │   └── utility.py
│       ├── gold
│       │   └── gold_app.py
│       ├── silver
│       │   └── silver_app.py
│       └── tests
│           ├── html
│           │   └── highscore_page.html
│           ├── __init__.py
│           ├── test_category.py
│           ├── test_highscore.py
│           └── test_vocation.py
├── README.md
└── services
    ├── lakehouse.yaml
    ├── observability.yaml
    ├── orchestration.yaml
    ├── processing.yaml
    └── visualization.yaml
```

## EM CONSTRUÇÃO

## FUTURAMENTE EVOLUIR PARA SESSÇÃO DE SKILLS (Magic Level, Axe, Sword, Distance e etc)
