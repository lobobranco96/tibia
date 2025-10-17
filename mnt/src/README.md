# Estrutura da Pasta `src`

Este diretório contém toda a lógica de código-fonte do projeto, organizada em camadas do pipeline de dados (padrão Lakehouse).

---

## Camada Bronze
Local: `src/bronze/`

A camada Bronze é responsável pela extração e ingestão de dados brutos do projeto.
Ela contém todos os scripts que realizam web scraping, tratamento inicial de dados e salvamento em MinIO ou S3-style storage, preparando os dados para serem processados nas camadas Silver e Gold.

### Principais módulos

- `extract.py`: Este módulo realiza a coleta de highscores do site oficial do Tibia.
  - Class Highscore:
    - Implementa o scraping da tabela principal de highscores do Tibia utilizando requests e BeautifulSoup. Retorna os dados em um DataFrame do pandas.
  - Class Vocation:
    - Extende o scraping para coletar dados por vocação (Knight, Paladin, etc.) e tipo de mundo, combinando resultados de múltiplas páginas em um único DataFrame.
  - Class Category:
    - Coleta dados de diferentes categorias de habilidades (como Sword Fighting, Magic Level), consolidando os dados em DataFrames padronizados.
    
- `utility.py`:
  - Responsável por salvar DataFrames como CSV na camada Bronze do MinIO, com particionamento por data (year/month/day).
  - Permite salvar localmente antes do upload (opcional).
  - Remove arquivos temporários automaticamente se configurado.
  - Suporta upload direto da memória sem criar arquivos locais.
  - Exemplo de uso:
    ```txt
    minio = CSVBronze()
    s3_path = minio.write(df, category_dir="experience", dataset_name="druid")
    # s3_path -> 's3a://bronze/year=2025/month=10/day=17/experience/druid.csv'
    ```
    
- `bronze_app.py`: ponto de entrada principal para o fluxo da camada Bronze.
  - Este script funciona como ponto de entrada da camada Bronze, coordenando:
    - Extração de dados do Tibia por vocação (Vocation) ou por categoria (Category).
    - Validação dos DataFrames (validate_csv) antes do envio.
    - Salvamento em MinIO (CSVBronze) com particionamento por data (year/month/day).
    - Logging detalhado de warnings e erros.

  - Funções principais:
    - extract_vocation(vocation: str) -> str
      - Extrai highscores de uma vocação específica e envia para o MinIO.
      - Retorna o caminho do arquivo no MinIO ou None em caso de falha.
  
    - extract_category(category: str) -> str
      - Extrai highscores de uma categoria (extra ou skills) e envia para o MinIO.
      - Retorna o caminho do arquivo no MinIO ou None em caso de falha.
        

    ```txt
      from bronze_app import extract_vocation, extract_category
    
      # Extrair highscores dos Knights
      extract_vocation("druid")
      
      # Extrair highscores de Sword Fighting
      extract_category("sword")

      # Extrair highscore extra
      extract_category("achievements")
    ```

---

## Camada Silver


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
