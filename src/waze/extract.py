import requests
import time
import pandas as pd
from datetime import datetime

# Função para obter dados da API com tentativas em caso de erro (exponential backoff)
def get_data_from_api(url, max_retries=3, backoff_factor=1):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Vai gerar erro se o status não for 200
            return response.json()  # Retorna o conteúdo como JSON
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Erro na requisição: {e}. Tentativa {retries}/{max_retries}")
            time.sleep(backoff_factor ** retries)  # Exponencial backoff
    return None  # Caso não consiga após 3 tentativas

# Função para processar os dados da API Waze
def process_waze_data(start_date, end_date):
    # URL da API Waze
    waze_url = "https://www.waze.com/row-partnerhub-api/partners/14420996249/waze-feeds/c5c19146-e0f9-44a7-9815-3862c8a6ed67?format=csv&types=alerts,traffic&fa=true"
    waze_data = get_data_from_api(waze_url)

    # Se os dados forem no formato esperado (JSON)
    if isinstance(waze_data, dict):  
        alerts_data = waze_data.get('alerts', [])
        if alerts_data:
            # Criar DataFrame com os dados dos alerts
            df_alerts = pd.json_normalize(alerts_data, sep='_')  # "Normaliza" a estrutura aninhada
            
            # Converter e garantir que a coluna de data seja datetime e timezone-aware
            df_alerts['pubMillis'] = pd.to_datetime(df_alerts['pubMillis'], errors='coerce', unit='ms')
            df_alerts['pubMillis'] = df_alerts['pubMillis'].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
            
            # Filtrando os dados no intervalo de tempo
            df_alerts = df_alerts[(df_alerts['pubMillis'] >= start_date) & (df_alerts['pubMillis'] <= end_date)]
            
            # Retornar o DataFrame com os alertas processados
            return df_alerts
        else:
            print("Nenhum dado de alertas encontrado.")
            return None
    else:
        print("Erro: Dados não estão no formato esperado (JSON).")
        return None

# Função principal para extração dos dados
def extract_and_save(start_date, end_date):
    # Processando os dados de alertas
    df_alerts = process_waze_data(start_date, end_date)

    if df_alerts is not None:
        # Adicionando uma coluna de data_evento para particionamento
        df_alerts['data_evento'] = df_alerts['pubMillis'].dt.date
        
        # Salvando os dados extraídos em um arquivo CSV (ou Parquet, se preferir)
        df_alerts.to_csv('waze_alerts_data.csv', index=False)
        # df_alerts.to_parquet('waze_alerts_data.parquet', partition_cols=['data_evento'])

        print(f"Dados extraídos e salvos com sucesso em 'waze_alerts_data.csv'!")
    else:
        print("Erro ao extrair dados da API Waze.")

if __name__ == '__main__':
    # Definindo intervalo de datas para a extração (exemplo)
    start_date = pd.to_datetime('2025-05-12T00:00:00-03:00')
    end_date = pd.to_datetime('2025-05-12T23:59:59-03:00')

    # Chamada para a função de extração e salvamento
    extract_and_save(start_date, end_date)


#============================================#======================================================================================#


# Explicação:
# Função extract_and_save():

# Organizei o processo de extração e salvamento em uma função única (extract_and_save) que orquestra a execução do script.

# A função process_waze_data() agora retorna um DataFrame (df_alerts), e se os dados forem válidos, a função segue para o salvamento em um arquivo.

# Data de entrada:

# Defini um intervalo de datas de exemplo com start_date e end_date. Essas variáveis podem ser ajustadas conforme necessário.

# A data de entrada agora é convertida para timezone-aware no formato correto usando pd.to_datetime().

# Salvar dados:

# Os dados são salvos em formato CSV por padrão (waze_alerts_data.csv), mas você pode facilmente substituir para salvar em formato Parquet (comentado no código).

# Particionamento por data_evento também foi adicionado para facilitar a organização dos dados.

# Execução:

# O bloco if __name__ == '__main__': assegura que o código seja executado quando o script for chamado diretamente.

# Como executar:
# Para rodar o script de extração, basta executar o seguinte comando:


# python src/waze/extract.py
# O script irá buscar os dados da API Waze e salvar os alertas em um arquivo CSV chamado waze_alerts_data.csv (ou Parquet, caso deseje).

# Considerações Finais
# Esse ajuste torna o script mais modular e adequado para execução independente.

# O processo agora está automatizado e pode ser facilmente escalado ou modificado para diferentes intervalos de data ou formatos de armazenamento.