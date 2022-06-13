# Spark
 banco de dados com spark apache

# link do databricks
    https://databricks.com/

# importações
from pyspark.sql.functions import max

# usando o dataframe
    arquivo = "dbfs:/databricks-datasets/flights/"
    df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").csv(arquivo)
    
# visualizando o tipo de dado
    df.printSchema()
    df.take(7)

    # para ver em formato de tabela
    display(df.show(7))

    # para saber o numero de linhas da tabela
    df.count()
    
    comando de max de uma coluna e pegando o primeiro item
    df.select(max("delay")).take(1)
    
    para utilização de filtros e where
    df.filter("delay < 2").show(5)
    df.where("delay < 2").show(5)

    usando ordenação
    df.orderBy(expr("delay desc")).show(10)

    describe da tabela
    df.describe().show()
    
    utilização de laços
    for i in df.collect():
        print(i)

# manipulando tabelas 
    add com parametro de delay + 2    
    df = df.withColumn('nova coluna', df['delay']+2)
    df.show(10)

    removendo coluna
    df = df.drop('nova coluna')

    renomeando uma coluna
    df.withColumnRenamed('nova coluna', 'New column').show()

# valores null
    filtra os valores nulos da coluna delay
    df.filter("delay is NULL").show()

    substitui null por 0 da coluna delay
    df.na.fill(value=0, subset=['delay']).show()
    
    preenche os dados com valores de string vazia
    df.na.fill("").show()

    remove qualquer linha nula de qualquer coluna
    df.na.drop().show()