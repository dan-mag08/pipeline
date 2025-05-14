import pandas as pd
from src.waze.parser import parse_waze_payload  # ajuste conforme o nome do seu módulo

def test_parser_retorna_schema_mesmo_com_campos_ausentes():
    # Payload com campos ausentes (ex: 'roadType' ausente)
    payload = [
        {
            "pubMillis": 1715600000000,
            "type": "ACCIDENT",
            "location": {"x": -43.2, "y": -22.9}
            # 'roadType' e outros campos ausentes
        }
    ]

    # Schema esperado (mesmo que venha com NaN)
    expected_columns = {
        "pubMillis",
        "type",
        "location.x",
        "location.y",
        "roadType"
    }

    # Função que transforma o payload em DataFrame
    df = parse_waze_payload(payload)

    # Garante que todas as colunas esperadas estão presentes, mesmo que ausentes no dado original
    assert expected_columns.issubset(set(df.columns)), \
        f"Schema retornado não contém todas as colunas esperadas. Encontrado: {set(df.columns)}"
