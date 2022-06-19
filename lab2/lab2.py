# Databricks notebook source
# Krista Miller

# Prime Exercise:
# In one In one databricks notebook command, create Spark code that counts the number of primes in a 
# given range. Start by first creating a Python list of all the numbers in the range 100..10,000. 
# Then use Spark commands to create a parallel RDD from this list. Using only Spark map, filter, 
# reduce and/or count, count the number of primes in this range in parallel. You may use 
# lambdas or standard Python functions in your maps/filters/reductions.

import numpy as np

sc = spark.sparkContext

a = list(range(100,10000))

aRdd = sc.parallelize(a)

def prime(x):
    if x == 1:
        return False
    elif x == 2:
        return True
    else:
        for i in range(2, x):
            if (x % i == 0):
                return False
        return True

primeRdd = aRdd.filter(prime)

print(primeRdd.count())


# COMMAND ----------

# Celsius Exercise:
# In one databricks notebook command, create Spark code that works with temperature in the 
# following way. Start with creating 1000 random Fahrenheit temperatures between 0..100 
# degrees F. This should be done in a standard Python list. Normally, we would load this data 
# from 1000 different observations, but for this lab we will simply generate random test data. 
# Next use Spark RDDs (only single ones – no pairRDDs) and only the Spark commands map, filter, 
# reduce and/or count to first convert the full list to Celsius. Then find the average of all the 
# Celsius values above freezing. You should print that average. You are only to use lambdas in 
# your maps/filters/reductions. And you should persist RDDs if helps reduce computations

b = list(np.random.randint(low= 0, high= 100, size= 1000))

bRdd = sc.parallelize(b)

#convert sample to celsius:
celsiusRdd = bRdd.map(lambda x: (x - 32)* 5/9)

#filter to above freezing:
above_freeze_celsiusRdd = celsiusRdd.filter(lambda x: x>0)

#sum all values in above freezing celsius sample:
resultSum = above_freeze_celsiusRdd.reduce(lambda x, y: x+y)

#calculate an average = resultSum/sample count:
print((resultSum)/above_freeze_celsiusRdd.count())

