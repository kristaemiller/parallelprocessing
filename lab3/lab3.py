# Databricks notebook source
## Krista Miller

#first make directory to store files. 
dbutils.fs.mkdirs("/FileStore/tables/lab3short")

#move or copy the files to that location with the mv or cp command.  write python code with a loop that creates the filePaths for the given files
fList =dbutils.fs.ls("/FileStore/tables/")

for i in range(0, len(fList)):
    if "shortLab" in fList[i].name:
        dbutils.fs.cp("/FileStore/tables/"+fList[i].name, "/FileStore/tables/lab3short/"+fList[i].name)

#confirm the files are in correct directory:
display(dbutils.fs.ls('/FileStore/tables/lab3short'))

# COMMAND ----------

sc = spark.sparkContext

#can pass in a directory name rather than a specific file name:
file = sc.textFile("/FileStore/tables/lab3short/")

#pair each target URL as the first item in the pair with the URL of a page which links it as a second element:
urls = file.map(lambda line: (line.split(" ")[0], line.split(" ")[1:]))

#mapValues to remove duplicates from value list and sort it:
def f(x): return sorted(list(set(x)))
urls2 = urls.mapValues(f)

#flatMapValues to return pair/value tuples of (target, reference):
def g(x): return x
urls3 = urls2.flatMapValues(g)

#map to flip target and references:
rddInverted = urls3.map(lambda x: (x[1], x[0]))

#group and sort by key:
final = rddInverted.groupByKey().mapValues(list).sortByKey(ascending = True)

#sort by value:
finalSorted = final.mapValues(f)

#output for small file:
print(finalSorted.collect())

# COMMAND ----------

#Now running for large file:

#first make directory to store files. 
dbutils.fs.mkdirs("/FileStore/tables/lab3full")

#move or copy the files to that location with the mv or cp command.  write python code with a loop that creates the filePaths for the given files
fList =dbutils.fs.ls("/FileStore/tables/")

for i in range(0, len(fList)):
    if "fullLab" in fList[i].name:
        dbutils.fs.cp("/FileStore/tables/"+fList[i].name, "/FileStore/tables/lab3full/"+fList[i].name)

#confirm the files are in correct directory:
display(dbutils.fs.ls('/FileStore/tables/lab3full'))

# COMMAND ----------

sc = spark.sparkContext

#can pass in a directory name rather than a specific file name:
file = sc.textFile("/FileStore/tables/lab3full/")

#pair each target URL as the first item in the pair with the URL of a page which links it as a second element:
urls = file.map(lambda line: (line.split(" ")[0], line.split(" ")[1:]))

#mapValues to remove duplicates from value list and sort it:
def f(x): return sorted(list(set(x)))
urls2 = urls.mapValues(f)

#flatMapValues to return pair/value tuples of (target, reference):
def g(x): return x
urls3 = urls2.flatMapValues(g)

#map to flip target and references:
rddInverted = urls3.map(lambda x: (x[1], x[0]))

#group and sort by key:
final = rddInverted.groupByKey().mapValues(list).sortByKey(ascending = True)

#sort by value:
finalSorted = final.mapValues(f)

#output for large file:
print(finalSorted.take(10))
print(finalSorted.count())
