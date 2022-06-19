# Databricks notebook source
sc = spark.sparkContext
from pyspark.sql import Row

baseballFile = "FileStore/tables/baseball/Master.csv"
baseballRdd = sc.textFile(baseballFile).map(lambda l: l.split(","))

#remove header from dataframe:
header = baseballRdd.first()
baseballRdd = baseballRdd.filter(lambda line: line != header)
#print(baseballRdd.collect())

#filter heights to digits; return only needed columns: playerID [0], birthCountry [4], birthState [5], and height [17] will be needed. Cast height to integer:
baseballRowsRdd = baseballRdd.filter(lambda x: x[17].isdigit()).map(lambda l: Row(playerid=l[0], birthCountry=l[4], birthState=l[5], height=int(l[17])))

#print(baseballRowsRdd.collect())


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

#create Schema with StructType rather than inferring one from the data as it is read in.  
baseballSchema = StructType([\
                            StructField('playerid', StringType(), True), \
                            StructField('birthCountry', StringType(), True), \
                            StructField('birthState', StringType(), True), \
                            StructField('height', IntegerType(), True)])

baseballDF = spark.createDataFrame(baseballRowsRdd, baseballSchema)
baseballDF.show()
baseballDF.printSchema()

# COMMAND ----------

#Colorado query:

#Find the number of players who were born in the state of Colorado.  You should do this both using SQL and again with DataFrame functions
baseballDF.createOrReplaceTempView("baseball")

#using SQL:
spark.sql("SELECT count(*) FROM baseball WHERE birthState = 'CO'").show()

# COMMAND ----------

#using DataFrame functions:
import pyspark.sql.functions as f

baseballDF.select(('*')).where(f.col('birthState') == 'CO').agg(f.count(f.lit(1)).alias("Colorado player count")).show()

# COMMAND ----------

#Height by Country query:

#List the average height by birth country of all players, ordered from highest to lowest.  You should do this both using SQL and again with DataFrame functions.

#using SQL:
spark.sql("SELECT birthCountry, AVG(height) FROM baseball GROUP BY birthCountry ORDER BY avg(height) DESC").show(baseballDF.count())

# COMMAND ----------

#using DataFrame functions:

baseballDF.select('birthCountry', 'height').groupBy('birthCountry').agg(f.avg('height')).orderBy(f.desc('avg(height)')).show(baseballDF.count())
