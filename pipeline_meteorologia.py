import argparse
from datetime import datetime
import pytz
import pandas as pd
from src.meteorologia.extract import extract_and_save  # Função de extração real
from src.meteorologia.transform import clean_and_convert, filter_by_date  # Funções de transformação
from src.meteorologia.load import save_meteorologia_to_parquet  # Função de persistência

def run_meteorologia_pipeline(start_date, end_date):
    """
    Pipeline para processar dados da API Meteorologia:
    1. Extrair os dados simulados da API e salvar CSV intermediário.
    2. Transformar os dados (limpeza e filtragem).
    3. Carregar os dados no formato Parquet.
    """

    print(f"Iniciando a extração de dados da Meteorologia para o intervalo {start_date} até {end_date}...")

    # Etapa 1: Extração
    extract_and_save()

    # Leitura do CSV gerado
    try:
        df_meteorologia = pd.read_csv('meteorologia_raw_data.csv')
    except FileNotFoundError:
        print("Arquivo meteorologia_raw_data.csv não encontrado. Finalizando pipeline.")
        return

    print(f"{len(df_meteorologia)} registros extraídos do arquivo.")

    # Etapa 2: Transformação
    print("Iniciando a transformação dos dados...")
    df_cleaned = clean_and_convert(df_meteorologia, 'read_at')
    df_filtered = filter_by_date(df_cleaned, start_date, end_date)

    print(f"{len(df_filtered)} registros após transformação.")

    # Etapa 3: Carga
    print("Iniciando a carga dos dados...")
    save_meteorologia_to_parquet(df_filtered, file_path="data/meteorologia_estacoes.parquet")

    print("Pipeline concluído com sucesso!")

def parse_args():
    parser = argparse.ArgumentParser(description="Executar o pipeline de dados da API Meteorologia.")
    parser.add_argument('--start-date', type=str, required=True, help="Data de início (formato: YYYY-MM-DD)")
    parser.add_argument('--end-date', type=str, required=True, help="Data de término (formato: YYYY-MM-DD)")
    return parser.parse_args()

def filter_by_date(df, start_date, end_date):
    """
    Filtra os dados por um intervalo de datas.
    """
    # Verifique se a coluna 'read_at' já tem fuso horário (tz-aware)
    if df['read_at'].dt.tz is None:
        # Se não tiver fuso horário, localize para o fuso horário correto
        df['read_at'] = pd.to_datetime(df['read_at']).dt.tz_localize('America/Sao_Paulo')
    else:
        # Se já tiver, converta para o fuso horário correto
        df['read_at'] = df['read_at'].dt.tz_convert('America/Sao_Paulo')

    # Filtra os dados entre as datas de início e fim
    df = df[df['read_at'].between(start_date, end_date)]
    return df

if __name__ == "__main__":
    args = parse_args()
    
    # Defina o fuso horário para UTC-03:00
    timezone = pytz.timezone('America/Sao_Paulo')
    
    # Converta as datas de início e fim e adicione o fuso horário
    start_date = timezone.localize(datetime.strptime(args.start_date, "%Y-%m-%d"))
    end_date = timezone.localize(datetime.strptime(args.end_date, "%Y-%m-%d"))
    
    run_meteorologia_pipeline(start_date, end_date)