# Projeto: Tibia Highscore Data Lakehouse

## Visão Geral

Este projeto tem como objetivo construir um Data Lakehouse completo, com orquestração via Apache Airflow, processamento distribuído em PySpark, armazenamento no MinIO (S3) e tabelas Iceberg para gerenciamento de dados versionados e consultas analíticas eficientes.

A pipeline foi projetada para coletar, transformar e disponibilizar dados de forma automatizada, garantindo escalabilidade, governança e reprodutibilidade em todas as etapas.

## Motivação

O Tibia possui rankings públicos que são atualizados constantemente, porém os dados não são disponibilizados de forma estruturada, histórica ou analítica.
Isso dificulta análises como:

- Evolução de jogadores ao longo do tempo
- Comparação entre vocações e tipos de mundo
- Análises históricas de ranking por skill ou categoria
- Criação de dashboards personalizados e reutilizáveis

Este projeto surge para resolver esse problema por meio de uma arquitetura de dados moderna, confiável e escalável, permitindo análises históricas e versionadas dos rankings do jogo.


## Objetivo

Extrair dados de rankings de jogadores, skills e outras categorias do Tibia.
Estruturar por:
 - Vocação: Knight, Paladin, Druid, Sorcerer, Monk e personagens sem vocação.
 - Tipo de mundo: Open PvP, Optional PvP, Hardcore PvP, Retro Open PvP, Retro Hardcore PvP.
 - Skills: Magic Level, Sword e etc
 - Extra: Achievements, Drome Score, Fishing e etc.
 - Garantir tasks independentes por vocação e categoria no Airflow, permitindo paralelismo e falhas isoladas.
 - Salvar dados de forma estruturada na camada Bronze, permitindo transformações em Silver e Gold.

# Decisão arquitetural
Mesmo com ingestões diárias pequenas, a camada Bronze foi construída sobre Apache Iceberg para garantir histórico, versionamento e consistência ao longo do tempo.

O Spark é utilizado não pelo volume atual dos dados, mas por ser o engine mais maduro para escrita transacional em Iceberg, integração com Nessie e evolução futura do pipeline.

Essa abordagem prepara o Lakehouse para crescimento contínuo, auditoria e análises temporais sem necessidade de refatoração estrutural.

## Fluxo

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

## Arquitetura do projeto

| Camada | Tecnologias | Descrição |
|--------|--------------|------------|
| **Coleta (Ingestão)** | **Python (Requests, BeautifulSoup)** | Automação e raspagem de dados web para coleta de informações brutas. |
| **Armazenamento (Landing)** | **MinIO (S3-compatible)** | Data Lake para armazenamento dos dados brutos |
| **Processamento (Bronze/Silver/Gold)** | **Apache Spark Cluster** | Processamento distribuído e transformação dos dados. |
| **File Format** | **Apache Iceberg** | Formato de tabela transacional com versionamento, schema evolution e time travel. |
|  **Catalogo de dados** | **Nessie Data Catalog** | Controle de versões e governança dos dados (Git para tabelas). |
| **Orquestração** | **Apache Airflow** | Coordena o pipeline de ponta a ponta (scraping → transformação → carga → dashboards). |
| **Consulta e Exploração** | **Dremio Query Engine** | SQL Engine para consultas sobre o Lakehouse (MinIO + Iceberg + Nessie). |
| **Testes e Exploração Local** | **Jupyter Notebook** | Ambiente de experimentação e validação de transformações. |
| **Visualização** | **Streamlit** | Dashboards interativos e análises visuais. |
|  **Monitoramento** | **Prometheus + Grafana** | Monitoramento e observabilidade de métricas (Spark, Airflow, containers, etc). |

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
A camada Bronze é responsável por estruturar os dados brutos provenientes da camada Landing, garantindo padronização, versionamento e auditabilidade.
 Nessa etapa:
  - Os arquivos CSV são lidos do MinIO, particionados por data.
  - As tabelas Iceberg são criadas automaticamente no catálogo Nessie.
  - São validadas colunas obrigatórias e aplicadas normalizações leves (tipos, textos e nomes).
  - Os dados recebem metadados de ingestão (batch_id, ingestion_time, ingestion_date).
  - Registros duplicados dentro do mesmo batch são removidos

A escrita é realizada de forma incremental (append), preservando o histórico completo.
Essa camada serve como base confiável e governada para as transformações nas camadas Silver e Gold.

### 3. Camada Silver:
A camada Silver é responsável por aplicar regras de negócio e versionar o histórico dos dados utilizando o padrão SCD Type 2.
Nessa etapa:
 - Os dados mais recentes da camada Bronze são lidos com base no último batch_id.
 - São criadas tabelas Iceberg no catálogo Nessie, caso não existam.
 - São geradas colunas de controle temporal (start_date, end_date, is_current).
 - Alterações nos registros são identificadas por meio de hash_diff.

É executado MERGE INTO para:
 - Encerrar versões antigas quando há mudanças.
 - Inserir novas versões mantendo o histórico.
 - Apenas um registro por chave de negócio permanece como atual (is_current = true).

A camada Silver garante rastreabilidade, histórico completo e consistência dos dados, servindo como base confiável para análises e agregações na camada Gold.

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

```text
┌─────────────────────────────┐
│   Extração / Scraping       │
│ (Airflow - Landing DAG)     │
└──────────────┬──────────────┘
               │
               v
┌─────────────────────────────────────────────┐
│               LANDING (MinIO)               │
│                                             │
│ landing/year=YYYY/month=MM/day=DD/          │
│ ├── vocation/                               │
│ │    ├── knight_*.csv                       │
│ │    ├── druid_*.csv                        │
│ │    └── _SUCCESS                           │
│ │                                           │
│ ├── skills/                                 │
│ │    ├── axe_*.csv                          │
│ │    ├── sword_*.csv                        │
│ │    └── _SUCCESS                           │
│ │                                           │
│ └── extra/                                  │
│      ├── achievements_*.csv                 │
│      ├── boss_*.csv                         │
│      └── _SUCCESS                           │
└──────────────┬──────────────┬───────────────┘
               │              │
               │              │
```
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

```text
        ┌───────────────────────────┐            ┌───────────────────────────┐        ┌───────────────────────────┐
        │   S3KeySensor (vocation)  │            │   S3KeySensor (skills)    │        │   S3KeySensor (extra)     │
        │ espera: vocation/_SUCCESS │            │ espera: skills/_SUCCESS   │        │ espera: extra/_SUCCESS    │
        └──────────────┬────────────┘            └──────────────┬────────────┘        └──────────────┬────────────┘
                       │                                        │                                    │
                       v                                        v                                    v
            ┌───────────────────────┐               ┌───────────────────────┐            ┌───────────────────────┐
            │ Spark Bronze Vocation │               │ Spark Bronze Skills   │            │ Spark Bronze Extra    │
            └───────────────────────┘               └───────────────────────┘            └───────────────────────┘



```
Objetivo: Processar os dados da camada Bronze e gerar tabelas versionadas nas camadas Silver e Gold, utilizando Spark, Iceberg e Nessie.
Dependência: É acionada automaticamente somente após os dados chegarem na Landing. Com isso o SparkSubmitOperator envia um comando spark-submit para o cluster Spark, iniciando a execução de um job PySpark customizado, responsável por processar os dados a partir dos arquivos da camada Landing e executar as transformações das camadas Bronze e Silver.

Detalhes de execução:
  - Cada categoria Bronze possui um job Spark independente:
  - Bronze Vocation > Silver Vocation.
  - Bronze Skills > Silver Skills.
  - Bronze Extra > Silver Extra.

Jobs Spark configurados com todos os jars necessários (AWS, Iceberg, Nessie) para garantir integração completa com MinIO/S3 e tabelas Iceberg.
Camadas envolvidas: Bronze > Silver > Gold (transformações, limpeza, agregações e versionamento).

Output: Tabelas Iceberg versionadas, auditáveis e prontas para consultas via Dremio ou dashboards.

```text
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
├── english_readme.md
├── Makefile
├── mnt
│   ├── airflow
│   │   └── dags
│   │       ├── lakehouse.py
│   │       └── landing.py
│   ├── minio
│   │   └── lakehouse
│   │       ├── bronze
│   │       ├── landing
│   │       └── silver
│   ├── notebooks
│   │   ├── iceberg_teste.ipynb
│   │   └── scraping.ipynb
│   ├── src
│   │   ├── jobs
│   │   │   ├── bronze_job.py
│   │   │   ├── gold_job.py
│   │   │   ├── silver_job.py
│   │   │   └── utils
│   │   ├── landing
│   │   │   ├── __init__.py
│   │   │   ├── landing_app.py
│   │   │   ├── __pycache__
│   │   │   ├── scraper.py
│   │   │   └── utility.py
│   │   ├── patch.txt
│   │   ├── __pycache__
│   │   │   ├── bronze_app.cpython-312.pyc
│   │   │   ├── extract.cpython-312.pyc
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   ├── teste.cpython-312.pyc
│   │   │   └── utility.cpython-312.pyc
│   │   ├── README.md
│   │   └── tests
│   │       ├── html
│   │       ├── __init__.py
│   │       ├── __pycache__
│   │       ├── test_category.py
│   │       ├── test_csvlanding.py
│   │       ├── test_highscore.py
│   │       └── test_vocation.py
│   └── tests_requirements.txt
├── README.md
├── services
   ├── lakehouse.yaml
   ├── observability.yaml
   ├── orchestration.yaml
   ├── processing.yaml
   ├── teste.yaml
   └── visualization.yaml
```

## Como Executar
Esta seção descreve como subir toda a infraestrutura do projeto localmente, inicializar os serviços e acessar cada componente do Lakehouse.

### Pré-requisitos
Antes de iniciar, é necessário ter instalado:
 - Docker (>= 20.x)
 - Make

### Configuração de Variáveis de Ambiente
As variáveis de ambiente utilizadas pelos serviços (Airflow, Spark, MinIO, Nessie, etc.) estão centralizadas no arquivo:
```bash
services/.credentials.env
```
Este arquivo contém, entre outras configurações:
 - Credenciais de acesso ao MinIO (S3)
 - Endpoint do Nessie Catalog
 - Configurações do Spark

### Inicialização do Ambiente ###
O projeto utiliza um Makefile para simplificar a execução e garantir consistência no build e na subida dos containers.
1. Build das imagens Docker:
Para construir todas as imagens customizadas (Airflow, Spark, Notebook, Prometheus, etc.):
```bash
make build
```
 Esse comando:
  - Builda todas as imagens definidas em docker/

2. Subida dos Containers
```bash
make up
```
ou
```bash
docker compose -f services/lakehouse.yaml up -d
docker compose -f services/orchestration.yaml up -d
docker compose -f services/processing.yaml up -d
docker compose -f services/observability.yaml up -d
```
Para iniciar todo o ambiente:
 - Inicializa todos os containers definidos em services/*.yaml
 - Sobe o cluster Spark
 - Inicializa Airflow, MinIO, Nessie, Dremio, Prometheus e Grafana
 - Cria a infraestrutura necessária para execução das DAGs e jobs Spark

## Serviços Disponíveis

Após a inicialização do ambiente, os seguintes serviços ficam acessíveis localmente:

| Serviço | URL | Descrição |
|-------|-----|-----------|
| **Apache Airflow (Web UI)** | http://localhost:8080 | Orquestração e monitoramento das DAGs |
| **MinIO (Console Web)** | http://localhost:9000 | Data Lake (Landing, Bronze, Silver, Gold) |
| **Nessie Catalog** | http://localhost:19120 | Catálogo e versionamento de tabelas Iceberg |
| **Apache Spark Cluster** | http://localhost:9090 | Monitoramento do cluster Spark |
| **Prometheus** | http://localhost:9091 *(se configurado)* | Coleta de métricas |
| **Grafana** | http://localhost:3000 *(se configurado)* | Dashboards de observabilidade |


## Considerações Finais

Este projeto demonstra a aplicação prática de uma arquitetura Lakehouse moderna,
focada em dados versionados, governança, automação e consumo analítico,
servindo como base para análises históricas e dashboards avançados do Tibia.


