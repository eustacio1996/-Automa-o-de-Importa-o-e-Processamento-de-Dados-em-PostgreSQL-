import pandas as pd
import os
import logging
import psycopg2
from sqlalchemy import create_engine

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para ler o CSV com tratamento de erros
def ler_csv(caminho_csv):
    try:
        # Leitura do CSV, ignorando linhas com erro e registrando
        df = pd.read_csv(caminho_csv, sep=';', on_bad_lines='skip')  #ignorar linhas mal formatadas
        logging.info(f"CSV lido com sucesso. NÃºmero de linhas: {len(df)}")
        logging.info(f"Primeiras linhas do CSV:\n{df.head()}")
        
        # Excluir a coluna 'Unnamed: 16' se existir
        if 'Unnamed: 16' in df.columns:
            df = df.drop(columns=['Unnamed: 16'])
        
        return df
    except Exception as e:
        logging.error(f"Erro ao ler o CSV: {str(e)}")
        raise

# Função para dividir o DataFrame em lotes e gravar em arquivos temporÃ¡rios
def dividir_em_lotes(df, batch_size=3000000):
    try:
        # Diretorio para arquivos temporÃ¡rios
        temp_dir = "caminho/para/o/arquivo"
        os.makedirs(temp_dir, exist_ok=True)
        
        arquivos_csv = []
        
        
        # Dividir o DataFrame em lotes e salvar cada lote em um arquivo CSV
        for start in range(0, len(df), batch_size):
            end = min(start + batch_size, len(df))
            batch = df.iloc[start:end]
            
            # Gerar o nome do arquivo CSV para cada lote
            batch_file = os.path.join(temp_dir, f"lote_{start}_{end}.csv")
            batch.to_csv(batch_file, index=False, sep=';', header=True)
            arquivos_csv.append(batch_file)
            logging.info(f"Lote de {start} a {end} salvo em {batch_file}.")
        
        return arquivos_csv
    except Exception as e:
        logging.error(f"Erro ao dividir os dados em lotes: {str(e)}")
        raise

# FunÃ§Ã£o para inserir dados no PostgreSQL com COPY
def inserir_no_postgresql_com_copy(lote_csv):
    try:
        # ConexÃ£o com o banco de dados
        conn = psycopg2.connect(
            host='localhost', 
            dbname='Database',
            user='usuario',
            password='senha'
        )
        
        # Usando o comando COPY para inserir os dados no PostgreSQL
        with conn.cursor() as cursor:
            # Definir o caminho absoluto para o arquivo CSV
            for lote in lote_csv:
                # Usar COPY para carregar os dados no PostgreSQL
                with open(lote, 'r') as f:
                    cursor.copy_expert(f"COPY tabela FROM stdin WITH CSV HEADER DELIMITER as ';'", f)
                    conn.commit()
                    logging.info(f"Dados do arquivo {lote} inseridos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao carregar os dados no PostgreSQL com COPY: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

# Caminho do arquivo CSV
caminho_csv = 'caminho/para/o/arquivo'

# Leitura e processamento dos dados
try:
    # Ler o CSV
    df = ler_csv(caminho_csv)
    
    # Dividir o DataFrame em lotes e salvar os arquivos temporÃ¡rios
    lotes_csv = dividir_em_lotes(df)
    
    # Inserir os dados no PostgreSQL com COPY
    inserir_no_postgresql_com_copy(lotes_csv)
    
except Exception as e:
    logging.error(f"Falha na execução: {str(e)}")
