# Pipeline — Tibia Highscore Data Lakehouse

## Visão Geral

O pipeline do projeto Tibia Highscore Data Lakehouse é responsável por orquestrar todo o fluxo de dados desde a extração via web scraping até a disponibilização de tabelas analíticas na camada Gold.

O desenho prioriza:

- Paralelismo
- Incrementalidade
- Isolamento de falhas
- Reprocessamento simples
- Observabilidade

---

## DAGs Principais

O projeto possui duas DAGs:

1. landing_highscores_pipeline  
2. lakehouse_pipeline  

---

## 1. landing_highscores_pipeline

Responsável pela extração dos dados do site do Tibia e gravação na camada Landing.

### Fluxo

```text
Extract Vocation
Extract Skills (categories)
        |
        v
Save CSV in Landing (MinIO)
```
### Características
  - Cada vocação é uma task independente
  - Cada skill é uma task independente
  - Cada categoria extra é uma task independente
  - Execução paralela
  - Retry automático

### Output

```text
s3://landing/year=YYYY/month=MM/day=DD/<categoria>/<arquivo>.csv
```

## 2. lakehouse_pipeline

```text
S3KeySensor
     |
Spark Bronze Job
     |
Spark Silver Job
     |
Spark Gold Job
```

### Características
  - Jobs Spark separados por domínio
  - Uso de SparkSubmitOperator
  - Escrita Iceberg + Nessie

## Camadas Envolvidas

Landing → Bronze
  - Leitura CSV
  - Padronização
  - Tipagem
  - Deduplicação
  - Inclusão de metadados

Bronze → Silver
  - Identificação de mudanças
  - SCD Type 2
  - MERGE INTO Iceberg

Silver → Gold
  - Inserção incremental
  - Tabelas analíticas

### Estratégia de Incrementalidade

```text
Camada	| Estratégia
Bronze	| Append
Silver	| MERGE
Gold	| INSERT
```

## Backfill

  - Qualquer tabela Gold pode ser reconstruída a partir da Silver.

## Tratamento de Falhas

  - Falhas não bloqueiam outras categorias
  - Retry configurado
  - Logs centralizados

## SLA

  - Pipeline diário.
  - Benefícios
  - Alta confiabilidade
  - Escalável
  - Reprocessável
