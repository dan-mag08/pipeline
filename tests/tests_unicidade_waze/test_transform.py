import pandas as pd
from src.waze.enrichment import clean_and_convert  # ajuste o path conforme necessário

def test_clean_and_convert_campos_ausentes():
    # Payload com campos ausentes (ex: coluna "location_x" está ausente)
    payload = pd.DataFrame({
        'pubMillis': ['2024-05-13T14:00:00+0000', '2024-05-13T15:00:00+0000'],
        'location_y': [-22.9, -22.95],  # faltando 'location_x'
        'descricao': ['Congestionamento', 'Acidente']
    })

    # Executa o parser/cleaner
    result = clean_and_convert(payload.copy(), 'pubMillis')

    # Schema esperado mesmo com campos ausentes
    expected_columns = {'pubmillis', 'location_y', 'descricao'}
    result_columns = set(result.columns)

    assert expected_columns == result_columns, f"Schema inesperado: {result.columns}"
