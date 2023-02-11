import psycopg2
import pandas as pd
import numpy as np

# gera dataframe aleatório
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

#cria a tabela se não existir
def create_table_if_not_exists(schema, table, conn):
    cursor = conn.cursor()
    cursor.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}') THEN
            CREATE TABLE {}.{} (
                id_processo SERIAL PRIMARY KEY,
                data DATE NOT NULL,
                status VARCHAR NOT NULL,
                tipo_da_acao VARCHAR NOT NULL,
                comarca VARCHAR NOT NULL,
                tipo_movimento VARCHAR NOT NULL,
                julgamento BOOLEAN NOT NULL
            );
        END IF;
    END$$;
    """.format(schema, table, schema, table))
    conn.commit()
    cursor.close()


# gera a tabela em sql
def insert_dataframe_into_postgres(df, table_name, conn):
    cursor = conn.cursor()
    data = [tuple(row) for i, row in df.iterrows()]
    cursor.executemany("""
        INSERT INTO desenv_processos.{} (id_processo, data, status, tipo_da_acao, comarca, tipo_movimento, julgamento)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """.format(table_name), data)
    conn.commit()
    cursor.close()

# configuração de credenciais
host = 'host.docker.internal'
user = 'postgres'
password = 'postgrespw'
port = '32768'


# esse postgres vem do docker aqui é gerado 10000 amostras para treinamento de SQL
conn = psycopg2.connect(host=host, user=user, password=password, port=32768, database="postgres")

#tamanho da amostra
df = get_dataset(10000)

create_table_if_not_exists("desenv_processo", "processos_movimento", conn)
insert_dataframe_into_postgres(df, "processos_movimento", conn)

conn.close()

# salva o csv na pasta csv
df.to_csv('../csv/dados.csv')