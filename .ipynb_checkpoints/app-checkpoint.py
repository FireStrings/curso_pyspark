#imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
from pyspark.sql.types import *

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


#Create spark session
spark = SparkSession.builder.appName("app_leo").getOrCreate()


arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("despachantes.csv", header=False, schema=arqschema)


calculo = despachantes.select("data").groupBy(f.year("data")).count()

calculo.write.format("console").save()

spark.stop()
