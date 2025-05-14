import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Função para persistir os dados do Waze em Parquet
def save_waze_to_parquet(df, file_path="data/waze_alertas.parquet"):
    print("Iniciando o processo de salvamento dos dados em Parquet...")
    
    # Garantir que o diretório para os arquivos Parquet exista
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Verificar se a coluna 'pubmillis' existe e é do tipo datetime
    if 'pubmillis' not in df.columns:
        print("Erro: Coluna 'pubmillis' não encontrada.")
        return
    
    # Convertendo a coluna 'pubmillis' para datetime se necessário (e.g., timestamp em milissegundos)
    if not pd.api.types.is_datetime64_any_dtype(df['pubmillis']):
        try:
            df['pubmillis'] = pd.to_datetime(df['pubmillis'], errors='coerce', unit='ms')
            print("Coluna 'pubmillis' convertida para datetime.")
        except Exception as e:
            print(f"Erro ao converter a coluna 'pubmillis' para datetime: {e}")
            return
    
    # Adicionar a coluna 'data_evento' para particionamento
    df['data_evento'] = df['pubmillis'].dt.date
    
    # Filtrando apenas dados novos (incremental) com base na última data registrada
    last_saved_date = None
    if os.path.exists(file_path):
        try:
            # Tentando ler o arquivo existente em Parquet
            existing_df = pd.read_parquet(file_path)
            last_saved_date = existing_df['data_evento'].max()  # Última data já salva
            df = df[df['data_evento'] > last_saved_date]  # Filtrando novos dados
            print(f"Última data salva: {last_saved_date}. Dados novos serão salvos.")
        except Exception as e:
            print(f"Erro ao ler arquivo Parquet existente: {e}")
            # Caso haja erro ao ler o Parquet, continuamos sem filtrar
            pass
    
    # Persistindo os dados em Parquet
    if not df.empty:
        try:
            # Convertendo para o formato Arrow (necessário para partições em Parquet)
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=file_path,
                partition_cols=['data_evento']  # Particionando por data_evento
            )
            print(f"Dados salvos com sucesso em: {file_path}")
        except Exception as e:
            print(f"Erro ao salvar os dados em Parquet: {e}")
    else:
        print("Não há novos dados para salvar.")

# Bloco condicional para execução do script diretamente
if __name__ == "__main__":
    # Aqui você pode carregar seu dataframe de exemplo ou de teste
    print("Carregando os dados para persistência em Parquet...")
    
    try:
        # Exemplo de carregamento de dados
        df_alerts_cleaned = pd.read_csv("waze_alerts_cleaned.csv")  # Ajuste conforme o nome do arquivo de entrada
        print("Dados carregados com sucesso!")
        
        # Chamando a função de persistência para salvar os dados em Parquet
        save_waze_to_parquet(df_alerts_cleaned)
    
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os dados: {e}")
