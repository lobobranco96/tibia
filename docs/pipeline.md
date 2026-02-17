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
Extract Vocation: no_vocation, knight, paladin, sorcerer, druid, monk.
Extract Skills: axe, sword, club, distance, magic_level, fist, shielding, fishing.
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
Exemplo de DataFrame:

| Rank | Name                | Vocation       | World     | Level | Points         | WorldType |
|------|--------------------|----------------|-----------|-------|----------------|-----------|
| 1    | Khaos Poderoso      | Master Sorcerer | Rasteibra | 2515  | 264,738,322,692 | Open PvP  |
| 2    | Goa Luccas          | Master Sorcerer | Inabra    | 2357  | 217,738,829,108 | Open PvP  |
| 3    | Syriz               | Master Sorcerer | Thyria    | 2189  | 174,396,658,081 | Open PvP  |
| 4    | Dany Ellmagnifico   | Master Sorcerer | Inabra    | 2160  | 167,580,849,914 | Open PvP  |
| 5    | Zonatto Bombinhams  | Master Sorcerer | Honbra    | 2132  | 161,212,779,898 | Open PvP  |


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
A camada Bronze é responsável por estruturar os dados brutos provenientes da camada Landing, garantindo padronização, versionamento e auditabilidade.
Nessa etapa:
  - Os arquivos CSV são lidos do MinIO, particionados por data.
  - As tabelas Iceberg são criadas automaticamente no catálogo Nessie.
  - São validadas colunas obrigatórias e aplicadas normalizações leves (tipos, textos e nomes).
  - Os dados recebem metadados de ingestão (batch_id, ingestion_time, ingestion_date).
  - Registros duplicados dentro do mesmo batch são removidos

A escrita é realizada de forma incremental (append), preservando o histórico completo.
Essa camada serve como base confiável e governada para as transformações nas camadas Silver e Gold.

Bronze → Silver
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

Silver → Gold
A camada Gold é a camada analítica final do Lakehouse, responsável por consolidar dados agregados e métricas prontas para consumo em dashboards e análises avançadas. Ela utiliza tabelas Iceberg versionadas, garantindo histórico, rastreabilidade e consultas eficientes.
Criação das tabelas da camada gold


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

## Screanshot
![Lakehouse Pipeline](docs/lakehouse_pipeline_success.png)
![Landing Highscore Pipeline](docs/landing__highscore_pipeline_success.png)
