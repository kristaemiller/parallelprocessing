# Databricks notebook source
#Krista Miller

#two files for this lab: heartTraining.csv, heartTesting.csv.  Both files contain a person id, age, sex and cholesterol level.  They both also have a field for a prediction of heart disease or not.  In the training set, these are the actual values, but in the testing set they are all set to a value of no.

from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.ml.feature import StringIndexer, Bucketizer
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

sc = spark.sparkContext

#load files:

trainingFile = "dbfs:///FileStore/tables/heartTraining.csv"
trainingDF = spark.read.format("csv").option("header", True).option("inferSchema", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(trainingFile)
trainingDF.show()
#trainingDF.printSchema()

testFile = "dbfs:///FileStore/tables/heartTesting.csv"
testDF = spark.read.format("csv").option("header", True).option("inferSchema", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load(testFile)
#testDF.show()
#testDF.printSchema()

# COMMAND ----------

#Building the pipeline:

#First you will build the ML pipeline on the training data.  This pipeline ends with a simple logistic regression on the data to determine if the person has heart disease or not. 

#cholesterol should be left as a number, sex and prediction string fields will need to be turned into numbers, age should be binned into categories

#age broken into categories: below 40, 40-49, 50-59, 60-69, 70 and above 

ageSplits=[-float("inf"), 40, 49, 59, 69, float("inf")]
ageBucketizer = Bucketizer(splits=ageSplits, inputCol= "age", outputCol= "ageBucket")

sexIndexer = StringIndexer(inputCol= 'sex', outputCol= 'sexIndex')

predIndexer = StringIndexer(inputCol='pred ', outputCol= 'label')


lr = LogisticRegression(maxIter=10, regParam=0.01)

vecAssem = VectorAssembler(inputCols= ['chol', 'ageBucket', 'sexIndex'], outputCol = "features")
                 
myStages = [ageBucketizer, sexIndexer, predIndexer, vecAssem, lr]

p = Pipeline(stages= myStages)

pModel= p.fit(trainingDF)

# COMMAND ----------

#Testing data:

#Once you have the pipeline model built, test it on the testing data.  As a final result, you should display the id, probability, and prediction data (just the first 20 lines are fine)

pred = pModel.transform(testDF)
pred.select('id', 'probability', 'prediction').show(20)

# COMMAND ----------

#You should also test it on the original training data to see how well the regression predictions match the human predictions.  With only these limited factors (age, sex, cholesterol) and no tuning of the regression, the predictions will not be highly accurate.  But for this lab, it will be fine.  Please include the measurement with your results

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# get the prediction
prediction = pModel.transform(trainingDF)

# # initialize the evaluator
evaluator = BinaryClassificationEvaluator()

# # # calculate AUC
auc = evaluator.evaluate(prediction, {evaluator.metricName: 'areaUnderROC'})

print(auc)
