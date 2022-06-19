# Databricks notebook source
#Krista Miller

sc = spark.sparkContext

pages = ["a b c", "b a a", "c b", "d a"]
rdd1 = sc.parallelize(pages)

#split on the blank space; collect set of x[1:] to remove duplicates to return in the form: [('a', ['b', 'c']), ('b', ['a']), ('c', ['b']), ('d', ['a'])]
links = rdd1.map(lambda x: (x.split(" ")[0], list(set(x.split(" ")[1:])))).persist()
print(f'Initial links: {links.collect()}')
    
#rankings holds the source page ranking information.  this will just be pairs collecting source page to its ranking
length = links.count()
rankings = links.map(lambda x: (x[0], 1/length))
print(f'Initial rankings: {rankings.collect()}')

for i in range(10):
    print(f'Iteration: {i}')
    
    joinedRdd = links.join(rankings)
    print(f'Initial rankings: {joinedRdd.collect()}')
    
    neighborContributions = joinedRdd.map(lambda x: x[1]).map(lambda x: (x[1]/len(x[0]), x[0])).flatMapValues(lambda x: x).map(lambda x: (x[1], x[0]))
    print(f'Neighbor contributions: {neighborContributions.collect()}')
    
    rankings = neighborContributions.reduceByKey(lambda x,y: x+y)
    print(f'New rankings: {rankings.collect()}')

print(f'Final sorted results: {rankings.sortByKey().collect()}')

# COMMAND ----------

#Lab3 short.  You should also do a test run on lab3short.  For this run, you can comment out the intermediate prints and just display the final sorted rankings. 

filePath = "/FileStore/tables/lab3short"

file = sc.textFile(filePath)

links = file.map(lambda x: (x.split(" ")[0], list(set(x.split(" ")[1:])))).persist()

length = links.count()
rankings = links.map(lambda x: (x[0], 1/length))

for i in range(10):
    joinedRdd = links.join(rankings)
    
    neighborContributions = joinedRdd.map(lambda x: x[1]).map(lambda x: (x[1]/len(x[0]), x[0])).flatMapValues(lambda x: x).map(lambda x: (x[1], x[0]))
    
    rankings = neighborContributions.reduceByKey(lambda x,y: x+y)
    
print(f'Final sorted results: {rankings.sortByKey().collect()}')
