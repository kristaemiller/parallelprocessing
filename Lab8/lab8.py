# Databricks notebook source
#Krista Miller

#we will be using Twitter tweets that occurred during the 2018 FIFA world cup as data for this lab.  We will be simulating a stream by creating a large number of files from this large file and placing them in a directory on the dbfs.  We will then use Spark Streaming to read its input from this one file at a time as our simulated stream.  So, the first step will be to load in the single large data and create the small files. 


# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

#Heres the schema for the CSV file:  
fifaSchema = StructType( \
 [StructField('ID', LongType(), True), \
 StructField('lang', StringType(), True), \
 StructField('Date', TimestampType(), True), \
 StructField('Source', StringType(), True), \
 StructField('len', LongType(), True), \
 StructField('Orig_Tweet', StringType(), True), \
 StructField('Tweet', StringType(), True), \
 StructField('Likes', LongType(), True), \
 StructField('RTs', LongType(), True), \
 StructField('Hashtags', StringType(), True), \
 StructField('UserMentionNames', StringType(), True), \
 StructField('UserMentionID', StringType(), True), \
 StructField('Name', StringType(), True), \
 StructField('Place', StringType(), True), \
 StructField('Followers', LongType(), True), \
 StructField('Friends', LongType(), True), \
 ])

soccerDF = spark.read.format("csv").option("header", True).schema(fifaSchema).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load("dbfs:///FileStore/tables/FIFA.csv")

soccerDF.show()

#put it into memory as a table so we can do sql on it later
soccerDF.createOrReplaceTempView("soccer")

# COMMAND ----------


#Then, keeping the full set of data as read, partition it to create many smaller partitions.  I created 50, but your code will run faster if you have less for testing.  Note that for testing purposes, you might want to order the date when you partition so the tweets are in order.  But, for your general streaming run, having the tweets come in out of order is a good test for your streaming code. 

soccerDF = soccerDF.repartition(50)
print(soccerDF.rdd.getNumPartitions())
#print(soccerDF.rdd.glom().collect())

# COMMAND ----------

dbutils.fs.rm("FileStore/tables/tweets", True)

soccerDF.write.format("csv").option("header", True).save("FileStore/tables/tweets")

# COMMAND ----------

#Static Windows

#The next thing you should do is put together your query in the static (non-streaming) environment.  It will be much easier to test statically on the full csv file you read initially.  

#The first thing you should do is reduce the columns you are working with.  You only need the ID, date, and hashtag columns.  Also, some of the tweets do not have hashtags and those will be null in the DataFrame.  So you can just filter those rows out at this point as well using isNotNull().  Note that it might be easier if you referenced column names with f.col('name') rather than fifaDF['name'] so your code is easier to reuse later in the streaming portion. 

# COMMAND ----------


staticTwitter = soccerDF.select('ID', 'Date', 'Hashtags').filter(f.col('Hashtags').isNotNull())

staticTwitter.show()



# COMMAND ----------

#when you look at the resulting DataFrame, you will see that certain rows have multiple hashtags like "WorldCup,FRA,ENG".  This was how the data for the tweet was created in the first place from the Twitter API.  Since we are going to be adding up all occurrences of each hash tag separately, we will need to have that comma separated string broken apart so each hashtag is on its own row.  To do this, you will use several DataFrame features.  

#The first is .withColumn.  That allows you to work with one column of the DataFrame.  You need to specify the column you want and what is going to happen to it.  What you need to do is to explode the column so you get a row for each element.  But explode takes a list of elements, not a comma separated string of elements.  So you will also have to use split which will create the list from the string.  Use the following example as a guide:

staticTwitter = staticTwitter.withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))
staticTwitter.show()


# COMMAND ----------

staticHashtagCount = staticTwitter.groupby("Hashtags").agg(f.count("Hashtags").alias("HashtagCount")).orderBy('HashtagCount')
staticHashtagCount.show()

# COMMAND ----------

#After you have the data transformed into a more useable form, you can then use DataFrame operations window a counting aggregation over the hashtags.  Your window should be a sliding window.  Each window should be 60 minutes long, with a sliding rate of every 30 minutes.  This will produce a large number of entries for each window since there are many single hashtags.  Since we only care about trending tags, you should only keep those that have aggregate counts greater than 100 within their window.  Note that in SQL you would do this with a HAVING clause to execute after your GROUP BY.  However, with DataFrames, you can simply do another .filter after the groupby agg.  That is, in SQL we needed two names: WHERE and HAVING since we didn't really specify the order.  But with DataFrame function, we get explicit control of the order and can get by with a single function: filter.  

#The ordering doesn't matter when you produce the resulting windowed DataFrame.  But it might be nice to see it ordered when showing it for human consumption.  Thus, when you show it, order it by window and count.  

# COMMAND ----------

trend =staticTwitter.groupBy((f.window("Date", "60 minutes", "30 minutes")), "Hashtags").agg(f.count("Hashtags").alias('HashtagCount')).filter(f.count("Hashtags") >100).orderBy(['window', 'HashtagCount'], ascending = [1,0])
trend.show()

# COMMAND ----------

#Structured Streaming

#Up to this point, you haven't done any streaming at all.  You have divided up the file so you can simulate streaming on the data.  And you have tested the transformation query you will use in a static environment where it is easier to test.  Now you are to code the streaming version.  Obviously, you will use your directory of small csv files as your sink.  The transformation query is similar to your static query.  And then the action is replaced with a sink.  Please use a memory sink. 

#The only additional issue is that we would like to have a watermark to prevent data that comes in way too late from being added to the stream.  Use 24 hours as your watermark value. 

#Once you start your streaming running, you can (in another command) issue a query to show the results as they are being produced by the streaming. 

# COMMAND ----------

#source

sourceStream = spark.readStream.format("csv").option("header", True).schema(fifaSchema).option("maxFilesPerTrigger",1).load("dbfs:///FileStore/tables/tweets")

staticTwitter = staticTwitter.withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))

#query
# trending = sourceStream.groupBy((f.window("Date", "60 minutes", "30 minutes")), "Hashtags").agg(f.count("Hashtags").alias('HashtagCount')).filter(f.count("Hashtags") >100).orderBy(['window', 'HashtagCount'], ascending = [1,0])

# trending = sourceStream.withColumn('Hashtags', f.explode(f.split('Hashtags', ','))).groupBy((f.window("Date", "60 minutes", "30 minutes")), "Hashtags").agg(f.count("Hashtags").alias('HashtagCount')).filter(f.count("Hashtags") >100).orderBy(['window', 'HashtagCount'], ascending = [1,0])

trending = sourceStream.withColumn('Hashtags', f.explode(f.split('Hashtags', ','))).withWatermark("Date", "24 hours").groupBy((f.window("Date", "60 minutes", "30 minutes")), "Hashtags").agg(f.count("Hashtags").alias('HashtagCount')).filter(f.count("Hashtags") >100).orderBy(['window', 'HashtagCount'], ascending = [1,0])

#sink
sinkStream = trending.writeStream.outputMode("complete").format("memory").queryName ("trending").trigger(processingTime = '10 seconds').start()

# COMMAND ----------

spark.sql("SELECT Window, Hashtags, HashtagCount FROM trending").show()
