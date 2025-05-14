import pandas as pd
from datetime import datetime

def save_meteorologia_to_parquet(df, file_path):
    """
    Salva o DataFrame em formato Parquet particionado por data_evento.
    """
    df['read_at'] = pd.to_datetime(df['read_at'], errors='coerce')
    df['data_evento'] = df['read_at'].dt.date
    df.to_parquet(file_path, partition_cols=['data_evento'])
    print("Dados carregados em Parquet com sucesso!")

if __name__ == '__main__':
    # Apenas para teste isolado
    df = pd.read_csv('meteorologia_raw_data.csv')
    save_meteorologia_to_parquet(df, 'meteorologia_estacoes.parquet')

