#=======================#==================================================================#=========================

# O arquivo utils.py pode ser utilizado para criar funções auxiliares, 
# como tratamento de exceções, formatação de dados, funções de retry, e 
# outras funções úteis que serão utilizadas em vários pontos do seu pipeline. 
# Aqui vou adicionar algumas funções que podem ser usadas no projeto.


import time
import requests
from datetime import datetime
import pytz

# Função de Retry Exponencial para APIs
def retry_request(url, max_retries=3, backoff_factor=1, params=None):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Vai gerar erro se o status não for 200
            return response.json()  # Retorna o conteúdo como JSON
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Erro na requisição: {e}. Tentativa {retries}/{max_retries}")
            time.sleep(backoff_factor ** retries)  # Exponential backoff
    return None  # Caso não consiga após 3 tentativas

# Função para converter datas para o formato desejado (timezone-aware)
def convert_to_timezone(df, date_column, timezone='America/Sao_Paulo', datetime_format="%Y-%m-%dT%H:%M:%S%z"):
    tz = pytz.timezone(timezone)
    
    # Convertendo para datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce', format=datetime_format)
    
    # Garantir que as datas estejam com fuso horário
    if df[date_column].dt.tz is None:
        df[date_column] = df[date_column].dt.tz_localize('UTC').dt.tz_convert(tz)
    else:
        df[date_column] = df[date_column].dt.tz_convert(tz)
    
    return df

# Função para verificar se a tabela já existe e filtrar os dados incrementais (caso a tabela Parquet já exista)
def get_existing_data(file_path):
    try:
        existing_df = pd.read_parquet(file_path)
        return existing_df
    except FileNotFoundError:
        return None

# Função para garantir que a pasta existe
def ensure_directory_exists(directory_path):
    import os
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Pasta {directory_path} criada com sucesso.")
