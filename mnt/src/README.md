# Estrutura da Pasta `src`

Este diretório contém toda a lógica-fonte do projeto, organizada seguindo o padrão arquitetural Lakehouse (camadas Landing > Bronze > Silver > Gold).

A pasta concentra:
- Scripts de web scraping,
- Tratamento inicial de dados,
- Ingestão no MinIO (S3-compatible),
- Processamento com Apache Spark (Iceberg),
- Transformações de versionamento (SCD2),
- Testes automatizados.
---

## Camada landing
Local: `src/landing/`

A camada Landing é responsável pela extração e ingestão de dados brutos do projeto.

### Principais módulos

- `scraper.py`: Este módulo realiza a coleta de highscores do site oficial do Tibia.
  - Class Highscore:
    - Implementa o scraping da tabela principal de highscores do Tibia utilizando requests e BeautifulSoup. Retorna os dados em um DataFrame do pandas.
  - Class Vocation:
    - Extende o scraping para coletar dados por vocação (Knight, Paladin, etc.) e tipo de mundo, combinando resultados de múltiplas páginas em um único DataFrame.
  - Class Category:
    - Coleta dados de diferentes categorias de habilidades (como Sword Fighting, Magic Level), consolidando os dados em DataFrames padronizados.
    
- `utility.py`:
  - Responsável por salvar DataFrames como CSV na camada Landing no diretorio local, com particionamento por data (year/month/day).
  - Exemplo de uso:
    ```txt
    minio = CSVLanding()
    s3_path = minio.write(df, category_dir="experience", dataset_name="druid")
    # s3_path -> 's3a://lakehouse/landing/year=2025/month=10/day=17/experience/druid.csv'
    ```
    
- `Landing_app.py`: ponto de entrada principal para o fluxo da camada Bronze.
  - Este script funciona como ponto de entrada da camada Bronze, coordenando:
    - Extração de dados do Tibia por vocação (Vocation) ou por categoria (Category).
    - Validação dos DataFrames (validate_csv) antes do envio.
    - Salvamento em MinIO (CSVLanding) com particionamento por data (year/month/day).
    - Logging detalhado de warnings e erros.

  - Funções principais:
    - extract_vocation(vocation: str) -> str
      - Extrai highscores de uma vocação específica e envia para o MinIO.
      - Retorna o caminho do arquivo no MinIO ou None em caso de falha.
  
    - extract_category(category: str) -> str
      - Extrai highscores de uma categoria (extra ou skills) e envia para o diretorio Landing que ja é integrado ao Bucket do Minio Landing ou seja, ao armazenar nesse diretorio os arquivos estão disponivel dentro do bucket .
      - Retorna o caminho do arquivo no MinIO ou None em caso de falha.
        

    ```txt
      from landing_app import extract_vocation, extract_category
    
      # Extrair highscores dos Knights
      extract_vocation("druid")
      
      # Extrair highscores de Sword Fighting
      extract_category("sword")

      # Extrair highscore extra
      extract_category("achievements")
    ```

---
## Camada Bronze
Local: `src/jobs/bronze_job.py`

A Bronze mantém dados quase brutos, porém estruturados em tabelas Iceberg, sendo o ponto de entrada do processamento distribuído no Spark.

Executado pelo Airflow via:
```bash
  spark-submit bronze_job.py vocation
  spark-submit bronze_job.py skills
  spark-submit bronze_job.py extra
```

O script lê sys.argv[1] e executa o processamento da categoria específica.

Funcionalidades principais:
  - Ingestão dos arquivos CSV da Landing Zone em tabelas Iceberg com particionamento por mundo e por dia de ingestão (days(ingestion_time)), para otimização de consultas.
  - Limpeza e padronização mínima, incluindo:
      - Cast dos tipos corretos (ex: level para INT, experience para LONG).
      - Remoção de caracteres indesejados.
      - Inclusão da timestamp de ingestão (ingestion_time).
      - Compressão Snappy habilitada para escrita parquet.
  - Modularidade para execução diária com parâmetro --date.


---

## Camada Silver
Local: `src/jobs/silver_job.py`

A camada Silver é responsável por aplicar regras de negócio e versionamento histórico dos dados utilizando o padrão Slowly Changing Dimension Type 2 (SCD2), com Apache Iceberg e Nessie como catálogo e controle de versões.

Os jobs são executados pelo Airflow via SparkSubmitOperator, de forma independente por domínio:
Executado pelo Airflow via:
```bash
  spark-submit silver_job.py vocation
  spark-submit silver_job.py skills
  spark-submit silver_job.py extra
```

Cada execução processa apenas o último batch gerado na camada Bronze, garantindo consistência e isolamento entre cargas.

### Funcionamento Geral da Silver
Leitura incremental dos dados da camada Bronze via batch_id.
Criação automática das tabelas Iceberg no namespace nessie.silver.
Geração de colunas de controle temporal:
  - start_date
  - end_date
  - is_current
    
Identificação de alterações por meio de hash_diff.
Aplicação de MERGE INTO para:
  - Encerrar versões antigas quando há mudanças.
  - Inserir novas versões mantendo o histórico.
  - Auditoria final com contagem total e registros atuais.

#### Tabela: vocation
Implementa SCD Type 2 para evolução de jogadores por vocação.

Versiona alterações em:
  - vocation
  - level
  - experience
  - world_type

Apenas um registro por (name, world) permanece ativo (is_current = true).
Particionamento otimizado para leitura e escrita:
  - world
  - days(start_date)
  - bucket(8, name)
    
Permite análises temporais completas da progressão de nível e experiência.

#### Tabela: skills
Aplica SCD Type 2 para habilidades individuais dos jogadores.

Histórico versionado por:
  - Jogador
  - Mundo
  - Categoria da skill (ex: sword, magic_level, shielding)
    
Controla mudanças em:
  - skill_level
  - level
  - vocation
    
Mantém apenas um registro atual por (name, world, category).
Estrutura de versionamento idêntica à tabela vocation.

---

## Camada Gold
Local: src/jobs/gold_job.py

A camada Gold é responsável por gerar tabelas analíticas finais, prontas para consumo em dashboards e análises, agregando dados versionados da camada Silver e aplicando cálculos de rankings e progressões.

As tabelas Gold são criadas como tabelas Iceberg no catálogo nessie.gold e suportam histórico com snapshots de data (snapshot_date) e timestamps de atualização (updated_at).

Jobs principais
O objeto Gold contém métodos para atualizar as tabelas Gold:

#### 1. experience_global_rank()

Gera o ranking global de experiência de jogadores.
Fonte: nessie.silver.vocation (apenas registros is_current = true)
Campos gerados:
  - rank: número do ranking global, ordenado por level DESC, experience DESC, name ASC.
  - name, world, vocation, level, experience, world_type
  - updated_at: timestamp da execução
Particionamento: por snapshot_date = DATE(current_timestamp())

#### 2. skills_global_rank()

Gera o ranking global de skills, particionado por categoria.
Fonte: nessie.silver.skills (apenas registros is_current = true)
Campos gerados:
  - rank: ranking dentro de cada categoria (PARTITION BY category)
  - name, world, category AS skill_name, vocation, skill_level
  - updated_at: timestamp da execução

#### 3. world_summary()

Cria um resumo da quantidade de jogadores por mundo e vocação.
Fonte: nessie.silver.vocation (apenas registros is_current = true)
Campos gerados:
  - world, world_type, vocation
  - players_count: quantidade de jogadores distintos
  - updated_at: timestamp da execução

#### 4. experience_progression()

Cria a tabela de evolução de level e experiência por jogador, com métricas de progresso.
Fonte: nessie.silver.vocation
Campos gerados:
  - name, world, vocation, world_type
  - previous_level, current_level, level_gain
  - previous_experience, current_experience, experience_gain
  - previous_start_date, current_start_date
  - days_between_updates, avg_xp_per_day
  - is_current, updated_at

#### 5. skills_progression()

Cria a tabela de progressão de skills por jogador, com métricas de ganho por período.
Fonte: nessie.silver.skills
Campos gerados:
  - name, world, vocation, category
  - skill_before, skill_after, skill_gain
  - from_date, to_date
  - days_between_updates, avg_skill_per_day
  - is_current, updated_at


---

## Testes
Local: `src/tests/`

Contém os testes automatizados do projeto.  
Os testes validam a lógica de extração, transformação e integridade dos dados.

### Estrutura
- `test_highscore.py`: testa a extração de rankings.
- `test_vocation.py`: testa as funções relacionadas a vocações.

---

## Observações

  - Todas as tabelas usam Apache Iceberg para versionamento e particionamento.
  - Nessie garante controle de versões e snapshots.
  - MinIO atua como armazenamento S3-compatible, integrado com Spark.
  - Logging detalhado e auditoria garantem rastreabilidade e consistência.
