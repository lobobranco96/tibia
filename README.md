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
| **Armazenamento (Landing)** | **MinIO (S3-compatible)** | Data Lake para armazenamento dos dados brutos |
| **Processamento (Bronze/Silver/Gold)** | **Apache Spark Cluster** | Processamento distribuído e transformação dos dados. |
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
  - Salvar em MinIO via CSVLanding (camada landing).

Exemplo de DataFrame final:

| Rank | Name                | Vocation       | World     | Level | Points         | WorldType |
|------|--------------------|----------------|-----------|-------|----------------|-----------|
| 1    | Khaos Poderoso      | Master Sorcerer | Rasteibra | 2515  | 264,738,322,692 | Open PvP  |
| 2    | Goa Luccas          | Master Sorcerer | Inabra    | 2357  | 217,738,829,108 | Open PvP  |
| 3    | Syriz               | Master Sorcerer | Thyria    | 2189  | 174,396,658,081 | Open PvP  |
| 4    | Dany Ellmagnifico   | Master Sorcerer | Inabra    | 2160  | 167,580,849,914 | Open PvP  |
| 5    | Zonatto Bombinhams  | Master Sorcerer | Honbra    | 2132  | 161,212,779,898 | Open PvP  |


### 2. Camada Bronze:
  - Criação das tabelas iceberg.
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

## Orchestração com Apache Airflow
O projeto utiliza duas DAGs principais para gerenciar o fluxo completo de dados, garantindo que a extração e o processamento sejam organizados, escaláveis e rastreáveis.

### 1 - DAG de Extração e Ingestão (landing_highscores_pipeline)
Objetivo: Coletar dados brutos do Tibia, por vocação, skills e categorias extras, e salvar na camada Landing (MinIO/S3) como CSVs particionados por data.
Detalhes de execução:
   - Cada vocação e categoria possui uma task independente, permitindo execução paralela.
   - Falhas em uma task não interrompem as demais, garantindo robustez.
   - Após a extração, os dados ficam prontos para processamento na camada Bronze.
   - 
Camadas envolvidas: Landing → Bronze (pré-processamento inicial, validação e organização dos CSVs).
Exemplo de tasks:
   - extract_vocation (none, knight, paladin, sorcerer, druid, monk)
   - extract_skills (axe, sword, club, distance, magic_level, fist, shielding)
   - extract_extra (achievements, fishing, loyalty, drome, boss, charm, goshnair)

Output: Arquivos CSV no MinIO organizados por:
```bash
s3://landing/year=YYYY/month=MM/day=DD/<categoria>/<nome>.csv
```

### 2 - DAG do Lakehouse (lakehouse_pipeline)

Objetivo: Processar os dados da camada Bronze e gerar tabelas versionadas nas camadas Silver e Gold, utilizando Spark, Iceberg e Nessie.
Dependência: É acionada automaticamente somente após a DAG de extração finalizar com sucesso. Isso é feito com o ExternalTaskSensor do Airflow.

Detalhes de execução:
  - Cada categoria Bronze possui um job Spark independente:
  - Bronze Vocation > Silver Vocation.
  - Bronze Skills > Silver Skills.
  - Bronze Extra > Silver Extra.

Jobs Spark configurados com todos os jars necessários (AWS, Iceberg, Nessie) para garantir integração completa com MinIO/S3 e tabelas Iceberg.
Camadas envolvidas: Bronze > Silver > Gold (transformações, limpeza, agregações e versionamento).

Output: Tabelas Iceberg versionadas, auditáveis e prontas para consultas via Dremio ou dashboards.

```txt
landing_highscores_pipeline (DAG de extração)
        |
        v
lakehouse_pipeline (DAG de processamento)
        |
        v
Bronze -> Silver -> Gold (Iceberg + Nessie)
```
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
│   │       ├── lakehouse.py
│   │       └── landing.py
│   ├── minio
│   │   ├── bronze
│   │   └── landing
│   │       └── year=2025
│   │           └── month=10
│   │               └── day=23
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
│   │                       ├── fist.csv
│   │                       ├── magic_level.csv
│   │                       ├── shielding.csv
│   │                       └── sword.csv
│   ├── notebooks
│   ├── requirements.txt
│   └── src
│       ├── jobs
│       │   ├── bronze
│       │   │   ├── extra.py
│       │   │   ├── skills.py
│       │   │   └── vocation.py
│       │   ├── gold
│       │   │   └── gold_app.py
│       │   ├── __init__.py
│       │   ├── silver
│       │   │   ├── extra.py
│       │   │   ├── skills.py
│       │   │   └── vocation.py
│       │   └── utility.py
│       ├── landing
│       │   ├── extract.py
│       │   ├── __init__.py
│       │   ├── landing_app.py
│       │   └── utility.py
│       ├── patch.txt
│       ├── README.md
│       └── tests
│           ├── html
│           │   └── highscore_page.html
│           ├── __init__.py
│           ├── test_category.py
│           ├── test_csvlanding.py
│           ├── test_highscore.py
│           └── test_vocation.py
├── README.md
├── services
    ├── lakehouse.yaml
    ├── observability.yaml
    ├── orchestration.yaml
    ├── processing.yaml
    └── visualization.yaml
```

## EM CONSTRUÇÃO

