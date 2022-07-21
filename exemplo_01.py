import re
import shutil

from pyspark.shell import spark
from pyspark.sql.functions import *
import spark_df_profiling
from pyspark.sql import *
from wordcloud import WordCloud, STOPWORDS
from operator import add
import matplotlib.pyplot as plt
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils

class spark_big_data():
    def __init__(self):
        self.rdd = None

    def run(self, arquivo, rdd=None, info=None, view=None, view_unique_column=None, profiling=None, cloud=None, search=None, analise=None, sql=None,
            nome_schema=None):
        schema = self.schema(arquivo, rdd, spark, nome_schema)
        if info:
            self.info_schema(schema)
        if view:
            self.view_schema(schema, sql)
        if profiling:
            self.profilling(schema)
        if view_unique_column:
            self.view_unique_column(schema, nameColumn=view_unique_column)
        if cloud:
            self.colud(end=end)
        if search:
            self.search(schema, search)
        if analise:
            self.contador(schema)

        return schema

    def schema(self, arquivo, rdd, spark, nome_schema):
        arquivo = arquivo

        if rdd:
            schema = spark.sparkContext.textFile(arquivo)
        elif rdd == False:
            df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("sep", ";").csv(
                arquivo, encoding='utf-8')
            schema = df.select("*").withColumn("id", monotonically_increasing_id())
            schema.cache()
        else:
            #gera uma sessão
            sparkSession = SparkSession.builder.appName("Schema_twitter").getOrCreate()
            # gera um dataframe com o melhor do drr e dataframe
            schema = sparkSession.read.option("header", "True").option("sep", ";").csv(arquivo)
            schema.createTempView(f'{nome_schema}')
            return schema
        self.rdd = rdd
        return schema

    def info_schema(self, schema):
        # informações do tipo de dados do banco
        schema.printSchema()

    def view_schema(self, schema, sql):
        #toda a tabela
        if self.rdd==False:
            schema.select("id", "tweet_text", "tweet_date", "sentiment", "query_used").show()
        elif self.rdd==False:
            print(schema.collect())
        elif sql:
            spark.sql(f'{sql}').show()

    def profilling(self, schema):
        schema.printSchema()
        schema.select("id", "tweet_text", "tweet_date", "sentiment", "query_used").show()
        self.contador(schema)

    def view_unique_column(self, schema, nameColumn=None):
        schema.select(nameColumn).show()

    def colud(self, end):
        texto = open(end, mode='r', encoding='utf-8').read()

        stopwords = STOPWORDS
        wc = WordCloud(
        background_color = 'white',
        stopwords = stopwords,
        height = 1000,
        width = 1000,
        )
        wc.generate(texto)
        wc.to_file(f'data/texto_doc.png')

    def search(self,schema, search):
        if self.rdd == True:
            texto = schema.filter(lambda x: f'{search}' in str(x).lower()) #lower aqui tem a função de normalização para minuscolo
            for i in texto.collect():
                print(f'\nA palavra pesquisada foi {search} no tweet {i}')
        else:
            texto = schema.select("id", "tweet_text", "sentiment", "query_used")
            for i in texto.collect():
                if re.search(f'\w{search}$', str(i), re.IGNORECASE):
                    print(f'\nA palavra pesquisada foi {search} no tweet {i}')

    def contador(self, schema):
        if self.rdd == True:
            texto_sem = ['a','e','o','da','de','do','das', 'dos', 'Wed', 'Aug', 'https', 'http', 'Fri', 'ele', 'não', 'um',
                         'uma', 'só', 'queria']
            filtro_palavras = schema.filter(lambda x: x not in texto_sem)
            freq_sentimento = filtro_palavras.map(lambda x: [x[-4],1])
            totalSentimento = freq_sentimento.reduceByKey(add)
            print(totalSentimento.collect())
        else:
            #aplicação de regex
            totalSentimentoPositivos = schema.filter(col("sentiment").rlike('1')).count()
            totalSentimentoNegativos = schema.filter(col("sentiment").rlike('0')).count()
            print(f"sentimentos positivos:{totalSentimentoPositivos}, sentimentos negativos: {totalSentimentoNegativos}")


if __name__ == "__main__":
    sp = spark_big_data()
    end = "./data/Test.csv"

    # nota só usar um metodo de >> | sql | rdd | dataframe |
    nome_schema = "schema_twitter"
    #analise com sql
    # sql = f"select count(sentiment) as qte, sentiment from {nome_schema} group by (sentiment)"
    sql = f"select * from {nome_schema}"
    df = sp.run(arquivo=end, view=True, sql=sql, nome_schema=nome_schema)
    

    # com analise ativa
    # df = sp.run(arquivo=end, analise=True)

    #view
    # df = sp.run(arquivo=end, analise=True)