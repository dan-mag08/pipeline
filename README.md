# Pipeline de Coleta de Dados - Waze e Meteorologia

Este projeto implementa um pipeline para coleta, transformação e carregamento (ETL) de dados provenientes de duas APIs: Waze (dados de tráfego e alertas) e Meteorologia (dados de estações meteorológicas). O objetivo é extrair os dados dessas APIs, transformá-los e armazená-los em arquivos Parquet ou em bancos de dados relacionais, como PostgreSQL ou MySQL.

## Estrutura do Projeto

A estrutura do projeto é organizada da seguinte forma:


```
meu_projeto/
├── data/                              # Dados utilizados no projeto
│   ├── raw/                           # Dados brutos
│   │   ├── waze_alerts_data.csv
│   │   ├── meteorologia_raw_data.csv
│   ├── processed/                     # Dados processados
│       ├── waze_alerts.parquet
│       ├── meteorologia_estacoes.parquet
│       ├── meteorologia_estacoes_cleaned.parquet
│
├── notebooks/                         # Notebooks Jupyter para análise e exploração
│   ├── waze_data_analysis.ipynb
│   ├── meteorologia_data_analysis.ipynb
│
├── pipelines/                         # Arquivos que executam os pipelines principais
│   ├── pipeline_waze.py
│   ├── pipeline_meteorologia.py
│
├── src/                               # Código-fonte principal do projeto
│   ├── __init__.py
│   ├── utils.py                       # Funções auxiliares reutilizáveis
│   ├── waze/                          # Módulo da API Waze
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   ├── utils.py
│   ├── meteorologia/                 # Módulo da API Meteorologia
│       ├── __init__.py
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       ├── utils.py
│
├── tests/                             # Testes automatizados
│   ├── tests_unicidade_waze/
│   │   ├── test_extract.py
│   │   ├── test_transform.py
│   │   ├── test_load.py
│   ├── tests_unicidade_meteorologia/
│       ├── test_extract.py
│       ├── test_transform.py
│       ├── test_load.py
│
├── venv/                              # Ambiente virtual Python 
├── requirements.txt                   # Dependências do projeto
└── README.md                          # Documentação do projeto
```




## Requisitos

Antes de executar o projeto, certifique-se de ter as dependências necessárias instaladas. Você pode instalar todas as dependências do projeto usando o `requirements.txt`:

### Instalação

1. Clone o repositório para o seu computador:

    ```bash
    git clone https://github.com/seu-usuario/meu_projeto.git
    cd meu_projeto
    ```

2. Crie e ative um ambiente virtual (opcional, mas recomendado):

    ```bash
    python -m venv venv
    source venv/bin/activate   # No Windows, use venv\Scripts\activate
    ```

3. Instale as dependências:

    ```bash
    pip install -r requirements.txt
    ```

## Estrutura dos Scripts

### Extração de Dados (`extract.py`)

A função `extract.py` coleta os dados das APIs de cada serviço (Waze ou Meteorologia). Ela utiliza a biblioteca `requests` para fazer requisições HTTP e obter os dados.

- **API Waze:** O script consulta a API do Waze, busca os alertas de tráfego e converte os dados para um formato legível.
- **API Meteorologia:** O script coleta as informações das estações meteorológicas e organiza os dados em um DataFrame do `pandas`.

### Transformação de Dados (`transform.py`)

O script `transform.py` realiza a limpeza e transformação dos dados obtidos:

- Converte colunas de data e hora para o formato correto.
- Substitui valores inválidos ou nulos por `NaN`.
- Normaliza os nomes das colunas para um formato consistente (`snake_case`).
- Realiza a filtragem dos dados de acordo com um intervalo de datas.

### Carga de Dados (`load.py`)

O script `load.py` é responsável pela persistência dos dados:

- **Parquet:** Os dados transformados são salvos em arquivos Parquet, com particionamento por `data_evento`.
- **Banco de Dados Relacional:** Caso seja necessário, os dados podem ser armazenados em um banco de dados como PostgreSQL ou MySQL.

### Pipelines (`pipeline_waze.py`, `pipeline_meteorologia.py`)

Os scripts `pipeline_waze.py` e `pipeline_meteorologia.py` são responsáveis por orquestrar todo o fluxo de ETL:

- Chamam as funções de **extração**, **transformação** e **carga**.
- Aplicam as transformações nos dados.
- Persistem os dados de forma eficiente.

Os pipelines podem ser executados diretamente da linha de comando com argumentos de data, conforme mostrado abaixo.


## Executando o Pipeline

```bash
python pipeline_waze.py --start-date 2025-03-01 --end-date 2025-03-07

--start-date: Data de início no formato AAAA-MM-DD.

--end-date: Data de término no formato AAAA-MM-DD.

Pipeline Meteorologia
O pipeline para Meteorologia pode ser executado da mesma forma, fornecendo um intervalo de dados:

python pipeline_meteorologia.py --start-date 2025-05-01 --end-date 2025-05-07


--start-date: Data de início no formato AAAA-MM-DD.

--end-date: Data de término no formato AAAA-MM-DD.

Testes
O projeto inclui testes unitários para garantir que todas as etapas de extração, transformação e carga funcionem corretamente. Os testes estão localizados na pasta testes/, e você pode executá-los usando o pytest:

pytest testes/

Dependências
O arquivo requirements.txt contém todas as bibliotecas necessárias para o projeto. As principais dependências incluem:

pandas: Para manipulação e análise de dados.

requests: Para fazer requisições HTTP e consumir APIs.

pyarrow: Para trabalhar com arquivos Parquet.

sqlalchemy: Para persistência em banco de dados SQL.

pytest: Para testes unitários.

Exemplo de conteúdo de requirements.txt:

pandas==1.5.3
requests==2.28.1
pyarrow==9.0.0
sqlalchemy==1.4.39
pytest==7.1.2