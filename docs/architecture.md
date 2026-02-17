## Visão Geral

Este documento descreve a arquitetura técnica do projeto Tibia Highscore Data Lakehouse, detalhando os componentes, responsabilidades, padrões adotados e decisões arquiteturais que sustentam o pipeline de dados de ponta a ponta.

A arquitetura segue o paradigma Lakehouse com separação clara entre armazenamento, processamento, catalogação, orquestração e consumo.

---

## Princípios Arquiteturais

  - Separação de responsabilidades
  - Versionamento em todas as camadas
  - Processamento incremental
  - Paralelismo por domínio
  - Reprocessamento simples (backfill)

## Diagrama Lógico
```text
            ┌────────────────────────────┐
            │   Web Scraping (Python)     │
            └──────────────┬─────────────┘
                           │
                           v
            ┌────────────────────────────┐
            │        MinIO (Landing)      │
            └──────────────┬─────────────┘
                           │
                           v
            ┌────────────────────────────┐
            │        MinIO (Bronze)       │
            └──────────────┬─────────────┘
                           │
                           v
            ┌────────────────────────────┐
            │        Apache Spark         │
            │   Bronze → Silver → Gold    │
            └──────────────┬─────────────┘
                           │
                           v
        ┌────────────────────────────────────┐
        │   Apache Iceberg + Nessie Catalog  │
        └──────────────┬─────────────────────┘
                       │
                       v
            ┌────────────────────────────┐
            │            Trino            │
            └──────────────┬─────────────┘
                           │
                           v
            ┌────────────────────────────┐
            │          Streamlit          │
            └────────────────────────────┘
```

## Componentes da Arquitetura

### Orquestração
**Apache Airflow**
Responsável por coordenar a execução das DAGs de ingestão e processamento.

Funções:
  - Agendamento
  - Dependências
  - Retry
  - Monitoramento

### Armazenamento (Data Lake)

**MinIO**
Armazena dados em formato S3 nas camadas:
  - Landing
  - Bronze
  - Silver
  - Gold

### Processamento Distribuído

**Apache Spark**
Responsável por:
  - Leitura de CSV
  - Escrita Iceberg
  - Aplicação de SCD Type 2
  - Criação de agregações e rankings

### Formato de Tabela

**Apache Iceberg**
Oferece:
  - ACID
  - Time Travel
  - Schema Evolution
  - Partition Evolution

### Catálogo de Metadados

**Project Nessie**
Funciona como um Git para tabelas:
  - Branches
  - Commits
  - Rollback
  - Versionamento

### Engine de Consulta

**Trino**
Consulta diretamente tabelas Iceberg via Nessie.

### Visualização

**Streamlit**
Consome dados via SQL e apresenta dashboards interativos.

### Observabilidade

**Prometheus**
**Grafana**
Coletam e exibem métricas de:
  - Airflow
  - Spark
  - Containers
  - Infraestrutura

### Estratégia de Versionamento
Todas as tabelas são Iceberg
Controladas pelo Nessie

Possuem:
  - ingestion_time
  - snapshot_date
  - is_current (Silver)
  - 
Permite:
  - Auditoria
  - Reprocessamento
  - Comparações temporais

### Estratégia de Escalabilidade

  - Paralelismo por categoria
  - Spark distribuído
  - Componentes desacoplados
  - Possibilidade de migração futura para cloud (GCP/AWS)

### Estratégia de Resiliência

  - Tasks independentes
  - Retry automático
  - Backfill possível a partir da Silver
  - Falhas isoladas

### Segurança

  - Credenciais via .env
  - Rede Docker isolada
  - Serviços expostos apenas localmente

### Benefícios da Arquitetura
  - Modular
  - Reprocessável
  - Versionada
  - Auditável
  - Pronta para crescimento
