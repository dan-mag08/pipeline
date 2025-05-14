import pandas as pd
import pyarrow.parquet as pq

def test_unicidade_station_read_at_no_parquet():
    # Lê o Parquet gerado (ajuste o caminho conforme necessário)
    table = pq.read_table('meteorologia_estacoes.parquet')
    df = table.to_pandas()

    duplicatas = df.duplicated(subset=['station', 'read_at'])
    assert not duplicatas.any(), "O arquivo Parquet contém registros duplicados com mesma estação e data de leitura."
