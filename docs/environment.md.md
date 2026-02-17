# Environment Setup

Este documento descreve como configurar, inicializar e validar todo o ambiente local do projeto **Tibia Lakehouse**, incluindo containers, variáveis de ambiente, serviços e verificações iniciais.

O ambiente é totalmente baseado em Docker e Docker Compose, permitindo execução local reproduzível.

---

## Visão Geral da Infraestrutura

O ambiente local provisiona os seguintes componentes:

- Apache Airflow (orquestração)
- Apache Spark Cluster (processamento distribuído)
- MinIO (S3-compatible Data Lake)
- Nessie Catalog (catálogo Iceberg)
- Trino (engine SQL)
- Streamlit (visualização)
- Prometheus + Grafana (observabilidade)
- Jupyter Notebook (exploração)

Todos os serviços se comunicam pela mesma rede Docker.

---

## Pré-requisitos

Antes de iniciar, certifique-se de ter instalado:

- Docker >= 20.x  
- Docker Compose v2  
- Make  

Verifique:

```bash
docker --version
docker compose version
make --version
```
--- 

Estrutura de Arquivos Relevante

```text
services/
├── lakehouse.yaml        # MinIO, Nessie, Trino
├── orchestration.yaml   # Airflow
├── processing.yaml      # Spark Cluster
├── observability.yaml   # Prometheus + Grafana
├── visualization.yaml  # Streamlit
└── .credentials.env     # Variáveis de ambiente

docker/
├── airflow/
├── spark/
├── notebook/
├── streamlit/
├── trino/
└── prometheus/
```

## Variáveis de Ambiente
Todas as variáveis ficam centralizadas em:
```text
services/.credentials.env
```

Exemplo de conteúdo:
```bash
# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
S3_ENDPOINT=http://minio:9000

# Nessie
NESSIE_ENDPOINT=http://nessie:19120
NESSIE_REF=main

# Spark
SPARK_MASTER_URL=spark://spark-master:7077

# Trino
TRINO_CATALOG=nessie

# Airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Nunca versionar este arquivo com credenciais reais.

--- 

## Build das Imagens
Construa todas as imagens customizadas:
```bash
make build
```

Este comando:
  - Builda imagens de Airflow, Spark, Notebook e Prometheus
  - Garante dependências Python corretas

## Criação da Rede Docker

Crie a rede compartilhada:
```bash
docker network create lakehouse
```
Execute apenas uma vez.


## Subida dos Containers

Forma recomendada:
```bash
make up
```

Ou manualmente:

```bash
docker compose -f services/lakehouse.yaml up -d
docker compose -f services/orchestration.yaml up -d
docker compose -f services/processing.yaml up -d
docker compose -f services/observability.yaml up -d
docker compose -f services/visualization.yaml up -d
```

## Verificação de Status

```bash
docker compose ps
```
## Logs de um serviço:

```bash
docker compose logs -f <service_name>
```

## Endpoints dos Serviços

| Serviço | URL | Descrição |
|-------|-----|-----------|
| **Apache Airflow (Web UI)** | http://localhost:8080 | Orquestração e monitoramento das DAGs |
| **MinIO (Console Web)** | http://localhost:9000 | Data Lake (Landing, Bronze, Silver, Gold) |
| **Nessie Catalog** | http://localhost:19120 | Catálogo e versionamento de tabelas Iceberg |
| **Apache Spark Cluster** | http://localhost:9090 | Monitoramento do cluster Spark |
| **Trino Query Engine** | http://localhost:8085 | Motor de consulta SQL sobre tabelas iceberg |
| **Streamlit** | http://localhost:8081 | Aplicativo para visualizar os dados |
| **Prometheus** | http://localhost:9091 | Coleta de métricas |
| **Grafana** | http://localhost:3000 | Dashboards de observabilidade |

## Inicialização Recomendada

1. Subir ambiente
2. Acessar Airflow
3. Ativar DAG landing_highscores_pipeline
4. Após sucesso, ativar lakehouse_pipeline
5. Acessar Streamlit

## Validações Iniciais

**MinIO**
  - Bucket landing
  - Bucket lakehouse
  - Pastas:
      - landing/
      - bronze/
      - silver/
      - gold/
      - metadata/extraction_logs/

**Nessie**

Verifique se o catálogo responde:
```bash
curl http://localhost:19120/api/v1/config
```

**Trino**
Teste simples:

```bash
SHOW CATALOGS;
SHOW SCHEMAS FROM nessie;
SHOW TABLES FROM nessie.gold;
```

**Spark**

Acesse UI:
```bash
http://localhost:9090
```

Verifique se:
  - Spark Master ativo
  - Workers conectados


## Reinicialização Completa

Parar tudo:
```bash
make down
```
Subir novamente:
```bash
make up
```

## Reset Completo
Remove containers, volumes e dados:
```bash
docker compose down -v
```
Use apenas se quiser reiniciar todo o ambiente do zero.


## Boas Práticas
  - Nunca rodar Spark fora do container
  - Sempre subir serviços via Makefile ou compose
  - Não alterar imagens em produção sem rebuild
  - Versionar somente código, nunca dados

## Resultado Esperado
Ao final da configuração:
  - DAGs executando
  - Dados chegando no MinIO
  - Tabelas Iceberg criadas no Nessie
  - Consultas funcionando via Trino
  - Dashboards Streamlit exibindo rankings
