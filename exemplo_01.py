import shutil

from pyspark.shell import spark
from pyspark.sql.functions import max, desc, asc, expr, min, monotonically_increasing_id, lit
import spark_df_profiling
from pyspark.sql import *
from wordcloud import WordCloud, STOPWORDS
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils

class spark_big_data():

    def run(self, arquivo, end=None, rdd=None, info=None, view=None, view_unique_column=None, profiling=None, cloud=None, search=None):
        schema = self.schema(arquivo, rdd)
        if info:
            self.info_schema(schema)
        if view:
            self.view_schema(schema)
        if profiling:
            self.profilling(schema)
        if view_unique_column:
            self.view_unique_column(schema, nameColumn=view_unique_column)
        if cloud:
            self.colud(end)
        if search:
            self.search(schema, search)
        return schema

    def schema(self, arquivo, rdd):
        arquivo = arquivo

        if rdd == True:
            schema = spark.sparkContext.textFile(arquivo)
            schema.cache()
        elif rdd == False:
            df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("sep", ";").csv(
                arquivo, encoding='utf-8')
            schema = df.select("*").withColumn("id", monotonically_increasing_id())
            schema.cache()
        else:
            schema = SparkSession.builder.appName("Schema_twitter").getOrCreate()

        return schema

    def info_schema(self, schema):
        # informações do tipo de dados do banco
        schema.printSchema()

    def view_schema(self, schema):
        #toda a tabela
        schema.select("id", "tweet_text", "tweet_date", "sentiment", "query_used").show()

    def profilling(self, schema):
        schema.printSchema()
        schema.select("id", "tweet_text", "tweet_date", "sentiment", "query_used").show()

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
        texto = schema.filter(lambda x: f'{search}' in x)
        print(texto.collect())


if __name__ == "__main__":
    sp = spark_big_data()
    end = "./data/Test.csv"
    df = sp.run(arquivo=end, rdd=True, search="lula")
