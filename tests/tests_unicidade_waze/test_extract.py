import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

import pytest
import pandas as pd
from src.waze.extract import extract_and_save


def test_extract_and_save():
    # Defina um intervalo de datas para testar a extração
    start_date = pd.to_datetime('2025-05-12T00:00:00-03:00')
    end_date = pd.to_datetime('2025-05-12T23:59:59-03:00')

    # Chame a função de extração
    extract_and_save(start_date, end_date)

    # Verifique se o arquivo foi criado
    assert os.path.exists('waze_alerts_data.csv'), "O arquivo 'waze_alerts_data.csv' não foi criado!"

    # Verifique se o arquivo CSV pode ser lido com pandas
    df = pd.read_csv('waze_alerts_data.csv')
    assert isinstance(df, pd.DataFrame), "O arquivo 'waze_alerts_data.csv' não é um DataFrame válido!"

    # Verifique se o DataFrame não está vazio
    assert not df.empty, "O arquivo 'waze_alerts_data.csv' está vazio!"

