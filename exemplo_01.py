import shutil

from pyspark.shell import spark
from pyspark.sql.functions import max, desc, asc, expr, min, monotonically_increasing_id, lit
import spark_df_profiling
from pyspark.sql import *
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils

class spark_big_data():

    def run(self, arquivo, end=None, info=None, view=None, view_unique_column=None, profiling=None, classificacao=None):
        schema = self.schema(arquivo)
        if info:
            self.info_schema(schema)
        if view:
            self.view_schema(schema)
        if profiling:
            self.profilling(schema)
        # if classificacao:
        #     self.classificacao(schema, end)
        if view_unique_column:
            self.view_unique_column(schema, nameColumn=view_unique_column)

        return schema

    def schema(self, arquivo):
        arquivo = arquivo
        df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("sep", ";").csv(
            arquivo, encoding='utf-8')
        schema = df.select("*").withColumn("id", monotonically_increasing_id())

        return schema

    def info_schema(self, schema):
        # informações do tipo de dados do banco
        schema.printSchema()

    def view_schema(self, schema):
        #toda a tabela
        schema.select("nome", "tipo", "marca", "categoria", "cor", "modelo").show()

    def profilling(self, schema):
        schema.printSchema()
        schema.select("nome", "tipo", "marca", "categoria", "cor", "modelo").show()

    # def classificacao(self, schema, end):
    #     #criar um aclassificador aqui

    def view_unique_column(self, schema, nameColumn=None):
        schema.select(nameColumn).show()

if __name__ == "__main__":
    sp = spark_big_data()
    end = "./data/base_info_produtos.csv"
    df = sp.run(arquivo=end, classificacao=True)


