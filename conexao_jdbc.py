import psycopg2
import pandas as pd
import numpy as np


def get_dataset(size):
    df = pd.DataFrame()
    df['id_processo'] = np.random.permutation(np.arange(1, size + 1)) + 1
    datas = pd.date_range('2016-01-01', '2023-01-01')
    df['data'] = np.random.choice(datas, size)
    df['status'] = np.random.choice(["Em andamento", "Concluído", "Arquivado"], size)
    df['tipo_da_acao'] = np.random.choice(["Ação Civil Pública", "Ação de Indenização", "Ação Penal"], size)
    df['comarca'] = np.random.choice(["Comarca de São Paulo", "Comarca de Osasco", "Comarca de Osasco", "Comarca de Santo André", "Comarca de São Bernardo do Campo"], size)
    df['tipo_movimento'] = np.random.choice(['Processo iniciado','Execução de sentença'], size)
    df['julgamento'] = np.random.choice([True, False], size)
    return df

def create_table_in_postgres(table_name, conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE desenv_processos.{} (
            id_processo SERIAL PRIMARY KEY,
            data DATE,
            status VARCHAR,
            tipo_da_acao VARCHAR,
            comarca VARCHAR,
            tipo_movimento VARCHAR,
            julgamento BOOLEAN
        )
    """.format(table_name))
    conn.commit()
    cursor.close()
def insert_dataframe_into_postgres(df, table_name, conn):
    cursor = conn.cursor()
    for index, row in df.iterrows():
        values = (row['id_processo'], row['data'], row['status'], row['tipo_da_acao'], row['comarca'], row['tipo_movimento'], row['julgamento'])
        cursor.execute("""
            INSERT INTO desenv_processos.{} (id_processo, data, status, tipo_da_acao, comarca, tipo_movimento, julgamento)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """.format(table_name), values)
    conn.commit()
    cursor.close()

host = 'host.docker.internal'
user = 'postgres'
password = 'postgrespw'
port = '32768'

conn = psycopg2.connect(host=host, user=user, password=password, port=32768, database="postgres")
df = get_dataset(10000)
create_table_in_postgres("processos_movimento", conn)
insert_dataframe_into_postgres(df, "processos_movimento", conn)
conn.close()

df.to_csv('./csv/dados.csv')