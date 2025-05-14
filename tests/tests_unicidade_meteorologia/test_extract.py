import pandas as pd
from src.meteorologia.extract import fetch_meteorologia_data

# Função auxiliar para tratar valores ausentes representados por "-" no payload da API
def tratar_valor(valor):
    return pd.NA if valor == "-" else valor

def test_parser_schema_com_campos_ausentes():
    # Coleta os dados brutos de meteorologia (formato GeoJSON com geometry e properties)
    features = fetch_meteorologia_data()

    # Converte os dados coletados para um DataFrame para facilitar a manipulação
    df_meteorologia = pd.DataFrame(features)

    # Extrai e transforma os dados relevantes para um novo DataFrame estruturado
    df_meteorologia_extracted = pd.DataFrame({
        # Extrai a longitude e latitude dos dados de coordenadas geográficas
        'longitude': df_meteorologia['geometry'].apply(lambda x: x['coordinates'][0]),
        'latitude': df_meteorologia['geometry'].apply(lambda x: x['coordinates'][1]),

        # Extrai os dados meteorológicos e aplica tratamento para valores ausentes
        'temperature': df_meteorologia['properties'].apply(lambda x: tratar_valor(x['data']['temperature'])),
        'humidity': df_meteorologia['properties'].apply(lambda x: tratar_valor(x['data']['humidity'])),
        'wind': df_meteorologia['properties'].apply(lambda x: tratar_valor(x['data']['wind'])),

        # Extrai o nome da estação e a data/hora da leitura
        'station': df_meteorologia['properties'].apply(lambda x: x['station']['name']),
        'read_at': df_meteorologia['properties'].apply(lambda x: x['read_at']),
    })

    # Define as colunas esperadas no DataFrame final
    expected_columns = ['longitude', 'latitude', 'temperature', 'humidity', 'wind', 'station', 'read_at']

    # Valida se o schema do DataFrame extraído está conforme o esperado
    assert list(df_meteorologia_extracted.columns) == expected_columns

    # Valida se valores ausentes foram corretamente convertidos para NaN
    assert pd.isna(df_meteorologia_extracted.loc[0, 'temperature']), "Campo 'temperature' deveria estar ausente (NaN)"
    assert pd.isna(df_meteorologia_extracted.loc[0, 'humidity']), "Campo 'humidity' deveria estar ausente (NaN)"
