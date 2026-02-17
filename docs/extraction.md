# Extraction Layer – Highscores Pipeline

## Visão Geral

A etapa de extração é responsável por coletar dados de highscores do site oficial do jogo Tibia, estruturar essas informações em DataFrames e armazená-las na camada Landing do Lakehouse em formato CSV, com particionamento por data e registro de metadados.
Essa etapa é executada de forma orquestrada pelo Apache Airflow.

## Arquitetura da Extração

A extração segue princípios de:
Single Responsibility Principle (SRP)
Injeção de dependências
Separação entre:
  - Comunicação HTTP
  - Parsing de HTML
  - Regras de scraping
  - Persistência de dados

Arquitetura em alto nível:

```lua
Airflow
  |
landing_app.py
  |
  +--> scraper.py (coleta + parsing)
  |
  +--> utility.py (validação + escrita + metadata)
  |
MinIO (Landing + Metadata)
```

## Componentes

### utility.py
Responsável por:
  - Escrita de CSV na camada Landing
  - Validação de DataFrames
  - Registro de metadados de extração

Principais classes:

**CSVLanding**
  - Converte DataFrame → CSV
  - Envia para storage S3 compatível (MinIO)

Particiona por:
```text
landing/year=YYYY/month=MM/day=DD/<categoria>/<dataset>_<timestamp>.csv
```

**ExtractionMetadataWriter**

Gera logs em JSON contendo:
  - Pipeline
  - Entidade
  - Subcategoria
  - Quantidade de linhas
  - Colunas
  - Timestamp

**validate_csv**

Garante que:
  - DataFrame não está vazio
  - Contém colunas esperadas (opcional)
  - Possui mínimo de linhas
  - 
### scraper.py

Responsável por todo o processo de scraping.

Componentes:

**BuscadorPagina**

  - Realiza requisições HTTP
  - Controle de retries
  - Timeout configurável
  - Headers customizados
    
**HighScoreParser**

  - Recebe HTML
  - Localiza tabela de highscores
  - Converte em DataFrame Pandas

**BaseScraper (classe abstrata)**

Define fluxo padrão:
  - Baixar página
  - Parsear HTML
  - Concatenar páginas

**VocationScraper**

  - Coleta highscores por vocação
  - Itera por tipos de mundo (Open PvP, Optional PvP, etc.)

**CategoryScraper**

Coleta skills
  - Ex: sword, magic_level, fishing


### landing_app.py
Arquivo que conecta:
Scrapers → Validação → Landing

Funções principais:
**extract_vocation(vocation)**

  - Recebe vocação
  - Executa scraping
  - Valida schema
  - Salva em landing/experience/

**extract_category(category)**

Define se é skills ou extra
  - Executa scraping
  - Valida
  - Salva na Landing

Este arquivo é diretamente chamado pelas DAGs do Airflow.

## Fluxo End-to-End

```text
Airflow DAG
   |
extract_vocation / extract_category
   |
Scraper
   |
BuscadorPagina -> HighScoreParser
   |
DataFrame consolidado
   |
validate_csv
   |
CSVLanding.write()
   |
MinIO (Landing)
   |
ExtractionMetadataWriter
   |
MinIO (Metadata)
```

## Validações e Qualidade

  - DataFrame vazio não é salvo
  - Colunas esperadas podem ser exigidas
  - Logs de warning e error em todos os pontos críticos
  - Isso evita que dados inválidos avancem para Bronze.

## Metadados de Extração

Cada execução gera um JSON em:
```text
metadata/extraction_logs/year=YYYY/month=MM/day=DD/
```
Campos principais:
  - pipeline
  - layer
  - entity
  - subcategory
  - rows
  - columns
  - extracted_at

Esses logs permitem auditoria e troubleshooting.

## Particionamento e Organização dos Arquivos

Particionamento

Landing:
```text
landing/year=YYYY/month=MM/day=DD/<categoria>/<dataset>_<timestamp>.csv
```
Metadata:
```text
metadata/extraction_logs/year=YYYY/month=MM/day=DD/<entity>/<subcategory>.json
```

## Tratamento de Erros e Logging
  - Try/except em todas as funções públicas
  - Logs estruturados
  - Retorno None quando falha

Garante que a DAG identifique falhas facilmente.


## Benefícios da Abordagem
  - Código modular
  - Fácil extensão para novas categorias
  - Fácil teste unitário
  - Observabilidade completa

