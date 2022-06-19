# Databricks notebook source
#Krista Miller
sc = spark.sparkContext

#we want to create a new DataFrame that holds all first and last names of the allStars, organized by team.  Specifically, the DataFrame should have the following columns and only have that information for players who are allStars

# columns: playerID, teamID, nameFirst, nameLast, teamName 

#use the spark.read command to load the information directly into a DataFrame.  Since all these columns are strings, we can just let the CSV reader infer the Schema

masterFile = "dbfs:///FileStore/tables/baseball/Master.csv"
masterDF = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(masterFile)
master = masterDF.select("playerID","nameFirst","nameLast").distinct()
#master.show()

teamFile = "dbfs:///FileStore/tables/baseball/Teams.csv"
teamDF = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(teamFile)
teams = teamDF.select("teamID","name").distinct()
#teams.show()

allStarFile = "dbfs:///FileStore/tables/baseball/AllstarFull.csv"
allStarDF =spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(allStarFile)
allStars = allStarDF.select("playerID", "teamID").distinct()
#allStars.show()

# COMMAND ----------

#Once the DataFrames are loaded, you should join them together appropriately.  For this lab, I am requiring you to perform all your operations with DataFrame functions.  You will want to get rid of duplicate information.  (distinct()) is good for this task.  You will also want to think about the order you do things to reduce the amount of data shuffles on joining. 

#inner join master with allStars:
x = master.join(allStars, ['playerID'], how = 'inner')

#inner join combined master/allStars with team:
y = x.join(teams, ['teamID'], how = 'inner')

# COMMAND ----------

#we want to create a new DataFrame that holds all first and last names of the allstars, organized by team.  Specifically, the DataFrame should have the following columns and only have that information for players who are allStars

#select columns in this order: playerID, teamID, nameFirst, nameLast, teamName 
#alias name as teamName
#order by name aka teamName

import pyspark.sql.functions as f

z = y.select('playerID', 'teamID', 'nameFirst', 'nameLast', f.col('name').alias('teamName')).orderBy('name')
z.show()

# COMMAND ----------

#saving as Parquet:

#after you have created the new DataFrame of information, you are to save it out as a Parquet file so it can be read in later for queries.  In particular, the types of queries we are expecting to happen on the data will be things like asking the names of all the allstars for a particular team.  Thus, it would be a good idea to partition by teamName when writing out the Parquet file.

z.write.format("parquet").mode("overwrite").partitionBy("teamName").save("baseballAllstars")

# COMMAND ----------

#Read and Query:

#lastly to test your Parquet file, in a separate command, read in the DataFrame you previously saved.  And perform the query: give the first and last names of all the Colorado Rockies allstars.  Note that you should specifically use the name "Colorado Rockies" rather than the internal teamID ("COL") since that is internal to the DataFrame and not supposed to be known for outside queries. 

#You should display both the number of all stars as well as showing the full list of them.  Note there should be 24. 

par = spark.read.format("parquet").load("dbfs:///baseballAllstars")

par.select("nameFirst", "nameLast").filter(f.col("teamName") == "Colorado Rockies").show(par.count())

# COMMAND ----------


