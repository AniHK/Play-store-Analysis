# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

/FileStore/tables/googleplaystore-1.csv

# COMMAND ----------

df = spark.read.load('/FileStore/tables/googleplaystore-1.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(5)

# COMMAND ----------

# DBTITLE 1,Check Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data cleaning
df = df.drop("size","Content Rating","Last Updated","Android Ver","Current Ver")

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df=df.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
.withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
.withColumn("Installs",col("Installs").cast(IntegerType()))\
     .withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
         .withColumn("Price",col("Price").cast(IntegerType()))

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql select * from apps

# COMMAND ----------

# DBTITLE 1,Top reviews given to app
# MAGIC %sql select App,sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top 10 installed apps
# MAGIC %sql select App,Type,sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc

# COMMAND ----------

# DBTITLE 1,Category wise Distribution
# MAGIC %sql select Category,sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top paid apps
# MAGIC %sql select App,sum(Price) from apps
# MAGIC where Type='Paid'
# MAGIC group by 1
# MAGIC order by 2 desc
