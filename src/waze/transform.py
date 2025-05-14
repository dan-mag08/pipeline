import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim

def enrich_waze_data(df_alerts):
    print("Iniciando o enriquecimento dos dados de Waze com informações geográficas...")
    
    geolocator = Nominatim(user_agent="waze_enrichment")
    
    # Verificar se as colunas 'location_x' (longitude) e 'location_y' (latitude) existem
    if 'location_x' not in df_alerts.columns or 'location_y' not in df_alerts.columns:
        print("Erro: colunas 'location_x' e/ou 'location_y' não encontradas.")
        return df_alerts
    
    def reverse_geocode(row):
        try:
            # Usando location_x como longitude e location_y como latitude
            latitude, longitude = row['location_y'], row['location_x']
            print(f"Geocodificando: lat={latitude}, lon={longitude}")
            location = geolocator.reverse((latitude, longitude), language='pt', exactly_one=True)
            if location:
                address = location.raw.get('address', {})
                suburb = address.get('suburb', None)
                zone = address.get('zone', None)
                print(f"Endereço encontrado: bairro={suburb}, zona={zone}")
                return suburb, zone
            else:
                print("Nenhuma informação de endereço encontrada.")
        except Exception as e:
            print(f"Erro no geocoding: {e}")
        return None, None
    
    # Enriquecer com bairro e zona
    print("Aplicando geocodificação para enriquecer os dados com bairro e zona...")
    df_alerts[['bairro', 'zona']] = df_alerts.apply(reverse_geocode, axis=1, result_type="expand")
    
    print(f"Enriquecimento concluído. {df_alerts.shape[0]} registros enriquecidos.")
    
    return df_alerts


def clean_and_convert(df, date_column, datetime_format="%Y-%m-%dT%H:%M:%S%z"):
    print(f"Iniciando a limpeza e conversão dos dados na coluna {date_column}...")
    
    # Converter colunas de data para datetime timezone-aware
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce', format=datetime_format)
    
    # Substituir valores inválidos por NULL (NaN)
    df.replace({"N/D": np.nan, "-": np.nan, "": np.nan}, inplace=True)
    
    # Convertendo vírgulas para ponto e depois para float nas colunas numéricas
    for col in df.select_dtypes(include=[object]).columns:
        if df[col].str.contains(',').any():
            print(f"Convertendo vírgulas para ponto na coluna {col}.")
            df[col] = df[col].str.replace(',', '.').astype(float)
    
    # Normalizando os nomes das colunas para snake_case
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    
    print(f"Limpeza e conversão concluídas. Dados limpos: {df.shape[0]} linhas e {df.shape[1]} colunas.")
    
    return df

# Bloco condicional para execução do script diretamente
if __name__ == "__main__":
    # Aqui você pode carregar seu dataframe de exemplo ou de teste
    print("Carregando os dados para transformação...")

    # Exemplo de carregamento de dados
    try:
        df_alerts = pd.read_csv("waze_alerts_data.csv")  # Ajuste conforme o nome e o caminho dos seus dados
        print("Dados carregados com sucesso!")
        
        # Aplicando as funções de transformação
        df_alerts_enriched = enrich_waze_data(df_alerts)
        df_alerts_cleaned = clean_and_convert(df_alerts_enriched, 'pubMillis')
        
        # Salve ou faça algo com os dados transformados
        df_alerts_cleaned.to_csv("waze_alerts_cleaned.csv", index=False)
        print("Dados transformados e salvos com sucesso em 'waze_alerts_cleaned.csv'.")
    
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os dados: {e}")
