# import pyspark class Row from module sql
from pyspark.shell import spark
from pyspark.sql.functions import max, desc, asc, expr, min
from pyspark.sql import *

class spark_big_data():

    def run(self, arquivo):
        schema = self.schema(arquivo)
        manipulacao = self.manipulacao(schema)

    def schema(self, arquivo):
        arquivo = arquivo
        df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").csv(arquivo)
        df.printSchema()
        return df

    def manipulacao(self, schema):
        valor_max = schema.select(max("valor")).take(1)
        valor_min = schema.select(min("valor")).take(1)
        print(valor_max)
        print(valor_min)
        return valor_max, valor_min

if __name__=="__main__":
    sp = spark_big_data()

    end = "./data/cotacaoValores.csv"
    sp.run(arquivo=end)