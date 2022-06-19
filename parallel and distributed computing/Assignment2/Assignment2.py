# Databricks notebook source
#Krista Miller
#Assignment 2

#For this assignment you will pick your own dataset and perform a streaming ML analysis of it.

# COMMAND ----------

#Machine Learning:

#You will have to pick something in your dataset to analyze.  This can be a simple binary classification or clustering or anything of your choice.

#Obviously, the raw data in the dataset will need to be transformed to work with whatever ML algorithm you pick.  Thus, you should create a pipeline to handle the fitting and transformations. 

#You will need a training set to train your ML algorithm.  You will want to split off a portion of your dataset to use in training.  Then the rest of the dataset can be used for testing. 

#You do not need to tune your ML algorithm parameters or compare and contrast different algorithms for this assignment. 


from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.ml.feature import StringIndexer, Bucketizer
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

sc = spark.sparkContext

#load file, drop columns with high percentage of null values:
File = "dbfs:///FileStore/tables/pancreas.csv"
fileDF = spark.read.format("csv").option("header", True).option("inferSchema", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(File).drop("benign_sample_diagnosis", "stage") 

fileDF.show()
fileDF.printSchema()



# COMMAND ----------

#split dataset into test and training:

train, test = fileDF.randomSplit([0.7, 0.3])

print(train.count())
print(test.count())

train.show()
test.show()
#train.select('diagnosis').distinct().show()

# COMMAND ----------

#Building the pipeline:
 

#First you will build the ML pipeline on the training data.  This pipeline ends with a simple logistic regression on the data to determine if the person has pancreatic cancer or not. 


#sex will need to be turned into numbers, age should be binned into categories

#age broken into categories: below 40, 40-49, 50-59, 60-69, 70 and above 
#creatinine, LYVE1, REG1B, TFF1, REG1A should be left as a number. 

ageSplits=[-float("inf"), 40, 49, 59, 69, float("inf")]
ageBucketizer = Bucketizer(splits=ageSplits, inputCol= "age", outputCol= "ageBucket")

#sex prediction fields
sexIndexer = StringIndexer(inputCol= 'sex', outputCol= 'sexIndex')

vecAssem = VectorAssembler(inputCols= ['creatinine', 'LYVE1', 'REG1B', 'TFF1', 'ageBucket', 'sexIndex'], outputCol = "features")

lr = LogisticRegression(featuresCol = 'features', labelCol = 'diagnosis', maxIter=8, regParam=0.02, elasticNetParam= 0.07)
                 
myStages = [ageBucketizer, sexIndexer, vecAssem, lr]

p = Pipeline(stages= myStages)

pModel= p.fit(train)

# COMMAND ----------

#Testing data:

#Once you have the pipeline model built, test it on the testing data.  As a final result, you should display the id, probability, and prediction data (just the first 20 lines are fine)

pred = pModel.transform(test)
# pred.show(20)
pred.select('sample_id', 'probability', 'prediction').show(20)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# get the prediction
predictions = pModel.transform(train)

# Select (prediction, true label) and compute test error

evaluator = MulticlassClassificationEvaluator(
    labelCol="diagnosis", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
print("Accuracy", accuracy)

# COMMAND ----------

#Streaming:

#Lastly, you are to incorporate Spark Streaming into your assignment.  That is, view the training set of data as 'historical' data and simply use it to build your fitted model.  Then the testing set of data should be streamed to your program.  You can always simulate streaming by breaking up a larger DataFrame into several smaller files and stream by reading 1 file per trigger.  Note you will still be using a ML pipeline model to transform your streaming data. 

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

#file path for streaming dataset
File = "dbfs:///FileStore/tables/pancreasStreaming.csv" 

#schema:  
pancreasSchema = StructType( \
 [StructField('sample_id', StringType(), True), \
 StructField('Date', StringType(), True), \
 StructField('patient_cohort', StringType(), True), \
 StructField('sample_origin', StringType(), True), \
 StructField('age', LongType(), True), \
 StructField('sex', StringType(), True), \
 StructField('diagnosis', LongType(), True), \
 StructField('stage', StringType(), True), \
 StructField('creatinine', DoubleType(), True), \
 StructField('LYVE1', DoubleType(), True), \
 StructField('REG1B', DoubleType(), True), \
 StructField('TFF1', DoubleType(), True), \
 ])

pancreasDF = spark.read.format("csv").option("header", True).schema(pancreasSchema).option("ignoreLeadingWhiteSpace", True).load("dbfs:///FileStore/tables/pancreasStreaming.csv")
#pancreasDF.show()

df2 = pancreasDF.withColumn('Date', f.to_timestamp('Date', 'd/M/yyyy H:mm'))
df2.show()

#put it into memory as a table so we can do sql on it later
df2.createOrReplaceTempView("p")
df2.printSchema()


# COMMAND ----------

#partition the dataset to simulate streaming
df2 = df2.repartition(10)
print(df2.rdd.getNumPartitions())

# COMMAND ----------

dbutils.fs.rm("FileStore/tables/patients", True)

df2.write.format("csv").option("header", True).save("FileStore/tables/patients")

# COMMAND ----------

staticPatients = df2.select('sample_id', 'Date', 'age', 'sex', 'creatinine', 'LYVE1', 'REG1B', 'TFF1', 'diagnosis')

staticPatients.show()


# COMMAND ----------

# Source
sourceStream=spark.readStream.format("csv").option("header",True).schema(pancreasSchema).option("ignoreLeadingWhiteSpace",True).option("mode","dropMalformed").option("maxFilesPerTrigger",1).load("dbfs:/FileStore/tables/patients")

streamingPancreas = pModel.transform(sourceStream).select('sample_id', 'probability', 'diagnosis','prediction')

display(streamingPancreas)

# COMMAND ----------

#Writeup:

#You will also provide a writeup of your project.  This should include a high-level description of your data.  This includes where you found it and what the data represents.  You should also discuss the ML problem you are trying to solve.  This includes what ML algorithms you used and how accurate their results were.  Some sample results along with the high-level analysis of your work would be good to include as well.  You should also discuss how you handled the streaming section of the project.  And lastly, please discuss any issues you ran into from cleaning data to applying ML algorithms to streaming. 

# COMMAND ----------

#Submission:

#You are to attach 3 documents to your submission.  The first is your Python source code(py).  Make sure to have your name in a comment at the top of your source code.  Second is your dataset.  Third is your writeup document as described above. 
