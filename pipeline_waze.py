from geopy.geocoders import Nominatim
import pandas as pd
import numpy as np
import argparse

def enrich_waze_data(df_alerts):
    geolocator = Nominatim(user_agent="waze_enrichment")

    if 'location_x' not in df_alerts.columns or 'location_y' not in df_alerts.columns:
        print("Erro: colunas 'location_x' e/ou 'location_y' não encontradas.")
        return df_alerts

    def reverse_geocode(row):
        try:
            latitude, longitude = row['location_y'], row['location_x']
            print(f"Geocodificando: lat={latitude}, lon={longitude}")
            location = geolocator.reverse((latitude, longitude), language='pt', exactly_one=True)
            if location:
                address = location.raw.get('address', {})
                return address.get('suburb', None), address.get('zone', None)
        except Exception as e:
            print(f"Erro no geocoding: {e}")
        return None, None

    df_alerts[['bairro', 'zona']] = df_alerts.apply(reverse_geocode, axis=1, result_type="expand")
    
    return df_alerts

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa o enriquecimento dos dados Waze com geolocalização.")
    parser.add_argument("--start-date", type=str, required=True, help="Data de início (formato YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="Data de fim (formato YYYY-MM-DD)")
    args = parser.parse_args()

    # Aqui você pode carregar os dados usando as datas passadas
    print(f"Executando enriquecimento de {args.start_date} até {args.end_date}")



#=====================================================#===========================================================================#

# Executando o pipeline:

# Ao rodar o script python pipeline_waze.py --start-date 2025-03-01 --end-date 2025-03-07, o script irá:

# Extrair os dados da API Waze para o intervalo de datas especificado.

# Transformar os dados (enriquecendo, limpando e convertendo).

# Carregar os dados em formato Parquet, particionados por data_evento.

# Passo 2: Como executar o script
# Agora, você pode rodar o script pipeline_waze.py diretamente da linha de comando, passando os parâmetros de data de início e data de término:

# bash

# python pipeline_waze.py --start-date 2025-03-01 --end-date 2025-03-07
# Exemplo de execução:
# O comando acima irá:

# Iniciar a extração dos dados entre 1º de março de 2025 e 7 de março de 2025.

# Transformar e limpar os dados de alertas.

# Salvar os dados transformados em um arquivo Parquet, particionado por data_evento.