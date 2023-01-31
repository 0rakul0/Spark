# Spark
 banco de dados com spark apache
 https://spark.apache.org/docs/latest/api/python/getting_started/install.html#using-pypi
 <hr>
pip install pyspark
<hr>
# Spark SQL<p>
pip install pyspark[sql]
<hr>
# pandas API on Spark<p>
pip install pyspark[pandas_on_spark] plotly
<hr>
 
 
 
# Hadoop
 https://acervolima.com/instalando-e-configurando-o-hadoop-no-modo-pseudo-distribuido-no-windows-10/
 https://www.youtube.com/watch?v=OwTUExSFL-Y
 
# link para download    
  Anaconda:   https://www.anaconda.com/products/individual<br>
  Spark:      https://spark.apache.org/downloads.html<br>
  Java JDK:   https://www.oracle.com/java/technologies/downloads/#java8   
  Hadoop:     https://hadoop.apache.org/release/3.3.0.html
  Hadoop/bin: https://drive.google.com/file/d/1zuT8G3D2JFkbkdv6fMhnhBOj8YSsgJc-/view 
  Config/arq: https://drive.google.com/file/d/1DUcsitCWp5RxLc4R-BVM6CRtK7Obf67M/view
# link do databricks
    https://databricks.com/

# Tutorial para instalação e configuração do pyspark
  --- Aula 01 - Conceitos Básicos de PySpark
  https://www.youtube.com/watch?v=WpIDLm9ow2o    
  
   --- Aula 02 - Download dos softwares Apache SPARK, Anaconda e Java(JDK)
  https://www.youtube.com/watch?v=qtEZXXrGkFk   
  
  --- Aula 03 - Instalar os softwares Java JDK, Anaconda, Pyspark
  https://www.youtube.com/watch?v=L3oWBs0aMvI    
  
  --- Aula 04 - Configuração das variáveis de ambiente no Windows 
  https://www.youtube.com/watch?v=WXnVnwy-Zko    
  
  --- Aula 05 - Executando os softwares Apache SPARK e PYSPARK
  https://www.youtube.com/watch?v=xKOSnC2pICo    


# link de apoio
    https://www.youtube.com/watch?v=WpIDLm9ow2o
    https://www.youtube.com/watch?v=0BY8KySBHwE
    https://www.youtube.com/watch?v=FTH8WJ-odmM

# importações
    from pyspark.sql.functions import max

# usando o dataframe
    arquivo = "dbfs:/databricks-datasets/flights/"
    df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("sep", ";").csv(arquivo)

# usando RDD -> Resilient Distributed Datasets
    schema = spark.sparkContext.textFile(arquivo)
    schema.cache()

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

## funções importantes

- lit
  - Creates a :class:`~pyspark.sql.Column` of literal value.
  
  ````py 
  df.select(lit(5).alias('height')).withColumn('spark_user', lit(True)).take(1)
    [Row(height=5, spark_user=True)]
  ````

- col
  - Returns a :class:`~pyspark.sql.Column` based on the given column name.'

```py
    col = column
    col('x')
    Column<'x'>
    column('x')
    Column<'x'>

```
- asc
  - Returns a sort expression based on the ascending order of the given column name.

- desc
  - Returns a sort expression based on the descending order of the given column name.

- sqrt
  - Computes the square root of the specified float value.

- abs
  - Computes the absolute value.

- max
  - Returns the maximum value of the expression in a group.

- min
  - Returns the minimum value of the expression in a group.

- max_by
  - Returns the value associated with the maximum value of ord.
    
```py
  
        df = spark.createDataFrame([("Java", 2012, 20000),
                                ("dotNET", 2012, 5000),
                                ("dotNET", 2013, 48000),
                                ("Java", 2013, 30000)],
                               schema=("course", "year", "earnings"))
    
        df.groupby("course").agg(max_by("year", "earnings")).show()
    
        +------+----------------------+
        |course|max_by(year, earnings)|
        +------+----------------------+
        |  Java|                  2013|
        |dotNET|                  2013|
        +------+----------------------+
  
```

- min_by
  - Returns the value associated with the minimum value of ord.

```py
  
        df = spark.createDataFrame([("Java", 2012, 20000),
                                    ("dotNET", 2012, 5000),
                                    ("dotNET", 2013, 48000),
                                    ("Java", 2013, 30000)],
                                   schema=("course", "year", "earnings"))
        
        df.groupby("course").agg(min_by("year", "earnings")).show()
        
        +------+----------------------+
        |course|max_by(year, earnings)|
        +------+----------------------+
        |  Java|                  2013|
        |dotNET|                  2013|
        +------+----------------------+
  
```

- count
  - Returns the number of items in a group.


- sum
  - Returns the sum of all values in the expression.


- mean ou avg
  - Returns the average of the values in a group.


- sum_distinct
  - Returns the sum of distinct values in the expression.


- product
  - col : str, :class:`Column`
        column containing values to be multiplied together

    Examples
```py
        df = spark.range(1, 10).toDF('x').withColumn('mod3', col('x') % 3)
        prods = df.groupBy('mod3').agg(product('x').alias('product'))
        prods.orderBy('mod3').show()
        
        +----+-------+
        |mod3|product|
        +----+-------+
        |   0|  162.0|
        |   1|   28.0|
        |   2|   80.0|
        +----+-------+
```
