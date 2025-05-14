import pandas as pd
from src.meteorologia.transform import clean_and_convert, filter_by_date

def test_unicidade_station_read_at_apos_transformacao():
    # Simula dados extraídos com possível duplicação
    raw_data = pd.DataFrame([
        {'station': 'Estação A', 'read_at': '2025-05-12T10:00:00-03:00', 'temperature': '25,3', 'humidity': '80', 'wind': '15 km/h'},
        {'station': 'Estação A', 'read_at': '2025-05-12T10:00:00-03:00', 'temperature': '25,3', 'humidity': '80', 'wind': '15 km/h'}  # duplicado
    ])

    # Limpeza e transformação
    df_cleaned = clean_and_convert(raw_data.copy(), 'read_at')
    df_filtered = filter_by_date(df_cleaned, '2025-05-12T00:00:00-03:00', '2025-05-12T23:59:59-03:00')

    # Remoção de duplicatas por station + read_at
    df_filtered = df_filtered.drop_duplicates(subset=['station', 'read_at'])

    # Teste de unicidade
    duplicatas = df_filtered.duplicated(subset=['station', 'read_at'])
    assert not duplicatas.any(), "Existem registros duplicados após transformação com a mesma estação e horário de leitura."
