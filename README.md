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

## Como funciona?

O pipeline começa com a **extração dos rankings de jogadores** do jogo Tibia. Os dados são coletados de forma automatizada, divididos por **vocação** e por **tipo de mundo**, garantindo confiabilidade e paralelismo.

### 1. Objetivo da Etapa
- Extrair os **rankings de jogadores** do Tibia.
- Organizar os dados por:
  - **Vocação**: Knight, Paladin, Druid, Sorcerer, Monk, e personagens sem vocação.
  - **Tipo de mundo**: Open PvP, Optional PvP, Hardcore PvP, Retro Open PvP, Retro Hardcore PvP.
- Garantir que cada vocação tenha uma **task independente** no Airflow, isolando falhas e permitindo execução paralela.

### 2. Organização das Tasks no Airflow
Cada vocação possui **uma task separada**:
1. `no_vocation`
2. `knight`
3. `paladin`
4. `sorcerer`
5. `druid`
6. `monk`

**Vantagens:**
- Falhas isoladas: se uma vocação falhar, as outras continuam.
- Paralelismo: várias vocações podem ser extraídas ao mesmo tempo.
- Debug simplificado: erros são facilmente localizados por vocação.

### 3. Fluxo Interno de Cada Task
1. **Gerar URLs de scraping**
   - Para cada vocação, são geradas URLs para **todas as páginas (1 a 20)** e tipos de mundo.
2. **Extrair dados da página**
   - Selenium carrega a página dinâmica.
   - BeautifulSoup localiza a tabela de highscores.
   - Dados de Rank, Name, Vocation, World, Level e Points são extraídos.
3. **Transformar em DataFrame**
   - A primeira linha da tabela vira o **cabeçalho**.
   - As demais linhas são os **valores dos jogadores**.
   - Exemplo:

| Rank | Name       | Vocation | World       | Level | Points     | WorldType       |
|------|------------|----------|------------|-------|-----------|----------------|
| 1    | PlayerOne  | Knight   | Antica     | 999   | 250000000 | Open PvP       |
| 2    | PlayerTwo  | Knight   | Thais      | 995   | 240000000 | Open PvP       |
| 3    | PlayerThree| Knight   | Ab'Dendriel| 990   | 230000000 | Open PvP       |

4. **Concatenar dados por mundo**
- Todos os DataFrames de diferentes tipos de mundo e páginas são unidos em **um único DataFrame por vocação**.

### 4. Salvando os Dados na Camada Bronze (MinIO)

Após extrair e consolidar os dados das diferentes vocações, o próximo passo é **armazenar os DataFrames resultantes** de forma estruturada na camada Bronze do Data Lake, utilizando **MinIO**.

#### Classe `CSVBronze`

A classe `CSVBronze` é responsável por:

- Receber um **DataFrame** do pandas contendo os dados extraídos.
- Salvar esse DataFrame como **CSV** no MinIO.
- **Particionar os arquivos por data** (`year/month/day`) para facilitar organização e versionamento.

