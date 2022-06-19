# Databricks notebook source
#Krista Miller
#Lab 9

# COMMAND ----------

import pyspark.sql.functions as f
from graphframes import * #install jar file into cluster library

# COMMAND ----------

#ROUTES ARE EDGES:

#the routes.csv file contains all the airline routes between airports anywhere in the world.  The fields are: airline, airlineID, sourceAirport, sourceAirportID, destinationAirport, destinationAirportID, codeshare, stops, planeType

#the file contains 59k routes.  every route in the dataset has 0 stops.  note that there may be more than one route between 2 cities if multiple airlines have the same route.  the important piece in this file are the sourceAirport and destinationAirport (IATA codes like 'DEN')

#AIRPORTS ARE VERTICES:

#second, these codes are from all over the world.  for this lab, we want to restrict routes to only those that start and end in the United States.  

#note the values stored in the id fields of the vertices DataFrame must match the values stored in the src and dst fields of the edges DataFrame when forming a GraphFrame

#the airports.csv file contains information about every airport in the world.  The fields are: airportID, name, city, country, IATA, ICAO, Lat, Long, Alt, timeZone, DST, databaseTimeZone, type, source.  The file contains 10k airports.  The fields we care about are country and the IATA (3 letter airport code):

airportsDF = spark.read.format("csv").option("header", False).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load("/FileStore/tables/airports.csv").toDF("airportID", "name", "city", "country", "IATA", "ICAO", "Lat", "Long", "Alt", "timeZone", "DST", "databaseTimeZone", "type", "source").persist()
routesDF = spark.read.format("csv").option("header", True).option("ignoreLeadingWhiteSpace", True).option("mode", "dropMalformed").load("/FileStore/tables/routes.csv").persist()


airportsDF = airportsDF.select(f.col('IATA'), f.col('country')).filter(f.col('country') == 'United States')

# remove duplicate routes
routesDF = routesDF.select(f.col('source airport').alias('src'), f.col('destination apirport').alias('dst')).distinct()

airportsDF.show(10)
routesDF.show(10)


# COMMAND ----------

#create graphframe

# Next you will need to create a DataFrame for the vertices. This should be a list of all the 
# vertices in your graph. That is (assuming there are no isolated vertices with no edges - which 
# there aren't for this lab – that would be odd having an airport that no one flew into), you need 
# to get all the distinct src and dst values from your edges. Your vertex DataFrame needs to have 
# a field called "id" to work properly with GraphFrames.
# Finally, create the GraphFrame from your vertex and edge DataFrames.
                                 

e = routesDF.join(airportsDF, airportsDF.IATA ==  routesDF.src, 'leftsemi').join(airportsDF, airportsDF.IATA ==  routesDF.dst, 'leftsemi')
v = e.select(f.col('src')).union(e.select(f.col('dst'))).select(f.col('src').alias('id')).distinct()
g = GraphFrame(v,e)
print(f'The number of airports: {g.vertices.count()}')
print(f'The number of routes between airports: {g.edges.count()}')

# COMMAND ----------

# Next, you should use Motifs to find all US airports that only have one way - but not round trip -
# flights to Denver (DEN). That is, airports where you can fly directly into DEN from but not back 
# there directly or the reverse. Note that this doesn't include airports that have no flights at all 
# (either to or from DEN). Your answer should be:
#     Airports with no direct roundtrip to or from DEN:
# +-----+
# | IATA|
# +-----+
# |[AIA]|
# |[ORF]|
# |[CDR]|
# +-----+

# COMMAND ----------

flyToDen = g.find("(a)-[e]->(b)").filter("b.id = 'DEN'")
#flyToDen.show()

# #fly out of denver:
flyOutDen = g.find("(a)-[e]->(b)").filter("a.id = 'DEN'")
#flyOutDen.show()


roundtrip = g.find("(a)-[e]->(b); (b)-[e2]->(a)").filter("a.id = 'DEN'")
#roundtrip.show()

#fly into l join round trip where round trip is null
oneWay = flyToDen.join(roundtrip, roundtrip.e2 ==  flyToDen.e, "leftanti").select("e.src") #results in CDR

#fly out of DEN j join round trip where round trip is null
oneOut = flyOutDen.join(roundtrip, roundtrip.e == flyOutDen.e, "leftanti").select("e.dst")#results in AIA and ORF

oneWay.show()
oneOut.show()

# COMMAND ----------

# And finally, you are to use the built-in shortest path algorithms to find all the US airports that 
# require 4 or more flights to get to from DEN. Your results should be:
# Airports that take 4 or more flights to get to from DEN:
# +----+----+
# |IATA|Hops|
# +----+----+
# | ANV| 4|
# | AUK| 4|
# | CEM| 4|
# | HPB| 4|
# | HSL| 4|
# | KAL| 4|
# | MLL| 4|
# | NUL| 4|
# | OOK| 4|
# | SHG| 4|
# | TNC| 4|
# | TOG| 4|
# | VAK| 4|
# | VEE| 4|
# | WNA| 4|
# | WSN| 4|
# | ARC| 5|
# | NME| 5|
# +----+----+

# COMMAND ----------

results = g.shortestPaths(landmarks = ["DEN"])

#explode the dataframe, filter to values greater than or equal to 4:
x = results.select(results.id, f.explode(results.distances))

#filter to values greater than or equal to 4, sort by value and id:
x = x.filter(x.value >= 4).sort(x.value, x.id)

#alias columns:
y = x.select(x.id.alias("IATA"), x.value.alias("Hops")).show()
