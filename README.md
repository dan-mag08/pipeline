Pipeline de Coleta de Dados - Waze e Meteorologia
Este projeto implementa um pipeline para coleta, transformação e carga (ETL) de dados provenientes de duas APIs: Waze (dados de tráfego e alertas) e Meteorologia (dados de estações meteorológicas). O objetivo é extrair os dados dessas APIs, transformá-los e armazená-los em arquivos Parquet ou em bancos de dados relacionais, como PostgreSQL ou MySQL.

Estrutura do Projeto
A estrutura do projeto é organizada da seguinte forma:


meu_projeto/
├── notebooks/                      # Para notebooks Jupyter, se necessário
│   ├── waze_data_analysis.ipynb    # Notebook para análise de dados do Waze
│   ├── meteorologia_data_analysis.ipynb # Notebook para análise de dados de meteorologia
├── src/                            # Scripts Python
│   ├── __init__.py                 # Torna a pasta 'src' um pacote Python
│   ├── utils.py                    # Funções auxiliares
│   ├── waze/                       # Pasta com scripts para a API Waze
│   │   ├── __init__.py
│   │   ├── extract.py              # Coleta de dados da API Waze
│   │   ├── transform.py            # Transformação dos dados da API Waze
│   │   ├── load.py                 # Persistência dos dados da API Waze
│   ├── meteorologia/               # Pasta com scripts para a API Meteorologia
│   │   ├── __init__.py
│   │   ├── extract.py              # Coleta de dados da API Meteorologia
│   │   ├── transform.py            # Transformação dos dados da API Meteorologia
│   │   ├── load.py                 # Persistência dos dados da API Meteorologia
├── pipeline_waze.py                # Pipeline principal para dados do Waze
├── pipeline_meteorologia.py        # Pipeline principal para dados de Meteorologia
├── requirements.txt                # Dependências do projeto
├── README.md                       # Documentação
└── tests/                          # Testes unitários
    ├── test_extract.py
    ├── test_transform.py
    ├── test_load.py

Requisitos
Certifique-se de que você tem as dependências necessárias instaladas. Você pode instalar todas as dependências do projeto usando o requirements.txt:

Clone o repositório para o seu computador:


git clone https://github.com/seu-usuario/meu_projeto.git
cd meu_projeto
Crie e ative um ambiente virtual (opcional, mas recomendado):


python -m venv venv
source venv/bin/activate  # No Windows, use `venv\Scripts\activate`
Instale as dependências:


pip install -r requirements.txt

Estrutura dos Scripts
1. Extração de Dados (extract.py)
A função extract.py coleta os dados da API de cada serviço (Waze ou Meteorologia). Ela utiliza a biblioteca requests para fazer requisições HTTP e obter os dados.

API Waze: O script consulta a API do Waze, busca os alertas de tráfego e converte os dados para um formato legível.

API Meteorologia: O script coleta as informações das estações meteorológicas e organiza os dados em um DataFrame do pandas.

2. Transformação de Dados (transform.py)
O script transform.py faz a limpeza e transformação dos dados obtidos:

Converte colunas de data e hora para o formato correto.

Substitui valores inválidos ou nulos por NaN.

Normaliza os nomes das colunas para um formato consistente (snake_case).

Realiza filtragem dos dados de acordo com um intervalo de datas.

3. Carga de Dados (load.py)
O script load.py é responsável pela persistência dos dados no formato Parquet ou no banco de dados:

Parquet: Os dados transformados são salvos em arquivos Parquet, com particionamento por data_evento.

Banco de Dados Relacional: Caso seja necessário, os dados podem ser armazenados em um banco de dados como PostgreSQL ou MySQL.

4. Pipelines (pipeline_waze.py, pipeline_meteorologia.py)
Os scripts pipeline_waze.py e pipeline_meteorologia.py são responsáveis por orquestrar todo o fluxo de ETL (extração, transformação e carga). Eles:

Chamam as funções de extração.

Aplicam a transformação nos dados.

Persistem os dados de forma eficiente.

Os pipelines podem ser executados diretamente da linha de comando com argumentos de data, como mostrado abaixo.

Executando o Pipeline
1. Pipeline Waze
O pipeline para o Waze pode ser executado usando o comando abaixo, onde você deve fornecer o intervalo de datas:


python pipeline_waze.py --start-date 2025-03-01 --end-date 2025-03-07
--start-date: Data de início no formato YYYY-MM-DD.

--end-date: Data de término no formato YYYY-MM-DD.

2. Pipeline Meteorologia
O pipeline para a Meteorologia pode ser executado da mesma forma, fornecendo um intervalo de datas:


python pipeline_meteorologia.py --start-date 2025-05-01 --end-date 2025-05-07
--start-date: Data de início no formato YYYY-MM-DD.

--end-date: Data de término no formato YYYY-MM-DD.

Testes

O projeto inclui testes unitários para garantir que todas as etapas de extração, transformação e carga estejam funcionando corretamente. Os testes estão localizados na pasta tests/, e você pode executá-los usando o pytest:


pytest tests/

Dependências
O arquivo requirements.txt contém todas as bibliotecas necessárias para o projeto. As principais dependências incluem:

pandas: Para manipulação e análise de dados.

requests: Para fazer requisições HTTP e consumir APIs.

pyarrow: Para trabalhar com arquivos Parquet.

sqlalchemy: Para persistência em banco de dados SQL.

pytest: Para testes unitários.

Exemplo de conteúdo do requirements.txt:
pandas==1.5.3
requests==2.28.1
pyarrow==9.0.0
sqlalchemy==1.4.39
pytest==7.1.2