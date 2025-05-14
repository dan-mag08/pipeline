import json
import requests
import pandas as pd

def fetch_meteorologia_data():
    # Simulação de coleta de dados (substitua com sua chamada real à API)
    features = [
        {'geometry': {'type': 'Point', 'coordinates': [-43.233056, -22.9925]}, 'type': 'Feature', 'properties': {'data': {'temperature': '-', 'min': '-', 'max': '-', 'humidity': '-', 'pressure': '-', 'wind': '14,4 (N)'}, 'station': {'id': 1, 'name': 'Vidigal'}, 'type': 'text', 'read_at': '2025-05-12T18:55:00-03:00'}},
        {'geometry': {'type': 'Point', 'coordinates': [-43.336944, -22.826944]}, 'type': 'Feature', 'properties': {'data': {'temperature': '23,6', 'min': '19,1', 'max': '28,0', 'humidity': '85', 'pressure': '-', 'wind': '-'}, 'station': {'id': 11, 'name': 'Irajá'}, 'type': 'text', 'read_at': '2025-05-12T18:55:00-03:00'}},
        {'geometry': {'type': 'Point', 'coordinates': [-43.223889, -22.972778]}, 'type': 'Feature', 'properties': {'data': {'temperature': '22,9', 'min': '19,1', 'max': '27,1', 'humidity': '100', 'pressure': '-', 'wind': '-'}, 'station': {'id': 16, 'name': 'Jardim Botânico'}, 'type': 'text', 'read_at': '2025-05-12T18:55:00-03:00'}}
    ]
    return features

def extract_and_save():
    # Coletar os dados
    features = fetch_meteorologia_data()
    
    # Converter os dados para DataFrame
    df_meteorologia = pd.DataFrame(features)
    
    # Extrair as informações relevantes
    df_meteorologia_extracted = pd.DataFrame({
        'longitude': df_meteorologia['geometry'].apply(lambda x: x['coordinates'][0]),
        'latitude': df_meteorologia['geometry'].apply(lambda x: x['coordinates'][1]),
        'temperature': df_meteorologia['properties'].apply(lambda x: x['data']['temperature']),
        'humidity': df_meteorologia['properties'].apply(lambda x: x['data']['humidity']),
        'wind': df_meteorologia['properties'].apply(lambda x: x['data']['wind']),
        'station': df_meteorologia['properties'].apply(lambda x: x['station']['name']),
        'read_at': df_meteorologia['properties'].apply(lambda x: x['read_at']),
    })

    # Salvar os dados em um arquivo CSV ou Parquet
    # Aqui, você pode ajustar para o formato que preferir
    df_meteorologia_extracted.to_csv('meteorologia_raw_data.csv', index=False)
    print("Dados extraídos e salvos com sucesso!")

if __name__ == '__main__':
    extract_and_save()


#===========================================#======================================================================================#


# Explicação:
# Função fetch_meteorologia_data(): Simula a coleta de dados. Se você tiver uma API, substitua essa parte com a chamada à API.

# Função extract_and_save(): Chama a função de coleta, converte os dados em um DataFrame, e então salva diretamente em um arquivo CSV (meteorologia_raw_data.csv), que será usado na próxima etapa (transformação).

# Execução do script: O bloco if __name__ == '__main__': garante que, quando você executar o script, a função extract_and_save() seja chamada.