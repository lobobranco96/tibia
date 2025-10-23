# Estrutura da Pasta `src`

- Este diretório contém toda a lógica de código-fonte do projeto, organizada em camadas do pipeline de dados (padrão Lakehouse).
- Contem  todos os scripts que realizam web scraping, tratamento inicial de dados e salvamento em MinIO ou S3-style storage, preparando os dados para serem processados nas camadas Silver e Gold.

---

## Camada landing
Local: `src/landing/`

A camada Landing é responsável pela extração e ingestão de dados brutos do projeto.

### Principais módulos

- `extract.py`: Este módulo realiza a coleta de highscores do site oficial do Tibia.
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
    # s3_path -> 's3a://landing/year=2025/month=10/day=17/experience/druid.csv'
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
Local: `src/bronze/`

A camada Bronze é responsável pelo armazenamento dos dados brutos, já convertidos para um formato otimizado, mas ainda mantendo o máximo de fidelidade da fonte.

Funcionalidades principais:
  - Ingestão dos arquivos CSV da camada Landing em tabelas Iceberg com particionamento por mundo e por dia de ingestão (days(ingestion_time)), para otimização de consultas.
  - Limpeza e padronização mínima, incluindo:
      - Cast dos tipos corretos (ex: level para INT, experience para LONG).
      - Remoção de caracteres indesejados.
      - Inclusão da timestamp de ingestão (ingestion_time).
      - Compressão Snappy habilitada para escrita parquet.
  - Modularidade para execução diária com parâmetro --date.
---

## Camada Silver
Local: `src/silver/`

A camada Silver aplica transformações mais complexas e cria históricos versionados.

Funcionalidades principais:
  - Implementação de Slowly Changing Dimension Type 2 (SCD Type 2) via comando MERGE INTO do Iceberg para versionar mudanças no vocacionário dos jogadores.
  - Registro histórico de alterações em nível, experiência, vocação e mundo, garantindo rastreabilidade dos dados ao longo do tempo.
  - Novos campos adicionados:
      - start_date: início do período do registro.
      - end_date: fim do período do registro.
      - is_current: flag para o registro ativo atual.
  - Particionamento otimizado por mundo, dia do start_date e bucket hash sobre o nome do jogador para balancear leitura/escrita.
  - Suporte para mudanças múltiplas em chaves naturais: nome, vocação e mundo, acompanhando alterações simultâneas.

---

## Camada Gold

---

## Testes
Local: `src/tests/`

Contém os testes automatizados do projeto.  
Os testes validam a lógica de extração, transformação e integridade dos dados.

### Estrutura
- `test_highscore.py`: testa a extração de rankings.
- `test_vocation.py`: testa as funções relacionadas a vocações.

---
