import pandas as pd
import numpy as np

# Função para converter datas e corrigir valores inválidos
def clean_and_convert(df, date_column, datetime_format="%Y-%m-%dT%H:%M:%S%z"):
    # Converter colunas de data para datetime timezone-aware
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce', format=datetime_format)
    
    # Substituir valores inválidos por NULL (NaN)
    df.replace({"N/D": np.nan, "-": np.nan, "": np.nan}, inplace=True)
    
    # Convertendo vírgulas para ponto e depois para float nas colunas numéricas
    for col in df.select_dtypes(include=[object]).columns:
        # Tratar a coluna 'wind' para extrair apenas o valor numérico
        if col == 'wind':
            df[col] = df[col].str.extract(r'([0-9,\.]+)').replace(',', '.', regex=True).astype(float)
        else:
            if df[col].str.contains(',').any():
                df[col] = df[col].str.replace(',', '.').astype(float)
    
    # Normalizando os nomes das colunas para snake_case
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    
    return df

# Função de filtragem temporal
def filter_by_date(df, start_date, end_date):
    # Converter as datas para o intervalo desejado
    df['read_at'] = pd.to_datetime(df['read_at'], errors='coerce')
    df = df[df['read_at'].between(start_date, end_date)]
    return df

def transform_and_save():
    # Carregar os dados extraídos (assumindo que o arquivo CSV foi gerado pela extração)
    df = pd.read_csv('meteorologia_raw_data.csv')
    
    # Limpeza e transformação dos dados meteorológicos
    df_cleaned = clean_and_convert(df, 'read_at')
    
    # Filtragem temporal
    start_date = '2025-05-12T00:00:00-03:00'
    end_date = '2025-05-12T23:59:59-03:00'
    df_cleaned = filter_by_date(df_cleaned, start_date, end_date)
    
    # Remover duplicatas com base em estação e data de leitura
    df_cleaned = df_cleaned.drop_duplicates(subset=['station', 'read_at'])

    # Adicionar uma coluna de data_evento para particionamento
    df_cleaned['data_evento'] = df_cleaned['read_at'].dt.date

    # Salvar os dados transformados em Parquet particionado por data_evento
    df_cleaned.to_parquet('meteorologia_estacoes_cleaned.parquet', partition_cols=['data_evento'])
    print("Dados transformados e salvos com sucesso!")

if __name__ == '__main__':
    transform_and_save()