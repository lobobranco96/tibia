# Visualization Layer — Streamlit Dashboards

Este documento descreve a camada de visualização do projeto Tibia Highscore Data Lakehouse, construída com Streamlit, consumindo dados diretamente das tabelas Iceberg na camada Gold através do Trino.

A camada de visualização fornece dashboards interativos para:
  - Rankings globais de experiência
  - Rankings globais de skills por categoria
  - Evolução histórica de jogadores
  - Evolução histórica de skills
  - Distribuição de jogadores por world e vocação

## Arquitetura da Visualização
```text
Iceberg Tables (Gold)
        |
        v
     Trino SQL
        |
        v
  Python Queries (pandas)
        |
        v
    Streamlit App
```
Cada página executa queries SQL via Trino, aplica filtros em memória com Pandas e renderiza tabelas e gráficos interativos.

## Global Player Ranking (Experience)

**Fonte de dados**:
  - nessie.gold.experience_global_rank

**Objetivo**:
  - Exibir ranking global diário de jogadores baseado em experiência e level.

### Funcionalidades

- Seleção de snapshot_date
- Filtro por:
  - World
  - World Type
  - Vocation

- Top N (10, 50, 100, 500, 1000)
- Busca por nome do jogador

- KPIs:
  - Total de jogadores
  - Level máximo e mínimo
  - Maior experiência
  - Quantidade de mundos
  - Data do snapshot

- Lógica de Ranking
- Deduplicação por (snapshot_date, name)
- Ordenação:
  - experience DESC
  - level DESC
  - name ASC
- Ranking recalculado dinamicamente no momento da consulta.

### Screenshot
![Global Experience Ranking](docs/streamlit_main_page.png)
![Experience Data](docs/streamlit_experience_rank_01.png)

## Global Skill Ranking

**Fonte de dados**:
  - nessie.gold.skills_global_rank

**Objetivo**:
  - Ranking global diário por categoria de skill.

### Funcionalidades

- Seleção de snapshot_date
Filtro por:
  - Categoria (skill)
  - World
- Top N
- Busca por jogador
KPIs:
  - Jogadores
  - Skill máxima e mínima
  - Worlds
  - Última atualização
  - Lógica de Ranking
  - Deduplicação por (snapshot_date, name, skill_name)

Ordenação:
  - skill_level DESC
  - name ASC

### Screenshot
![Skills Ranking](docs/streamlit_skills_page.png)
