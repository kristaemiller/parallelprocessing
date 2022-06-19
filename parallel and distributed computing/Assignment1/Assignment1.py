# Databricks notebook source
#Krista Miller
#Assignment 1

#In processing geographic data, we may have points which need to interact with geographically nearby points but do not interact with points beyond a certain distance.  

#The first step in solving this problem would be to generate a list of "close pair" points that are within the threshold distance of each other.  

#Next, we would run through the list and perform whatever calculation we wanted between those point pairs.  

#The input is a set of CSV text files.  Each line in each file contains the ID of a point, the floating-point X coordinate, and the floating-point Y coordinate, separated by commas. Ex:

# Pt04,4.84535,4.05086

# COMMAND ----------

sc = spark.sparkContext
import math

#can pass in a directory name rather than a specific file name:
file = sc.textFile("/FileStore/tables/assignment1")

#returns points in format ('Pt00', corrd1, coord2)
points = file.map(lambda x: x.split(",")).map(lambda x: (x[0], float(x[1]), float(x[2])))  
#points.collect()

# COMMAND ----------

#user defined distance
d = 0.75

#create grid of points
pointsGrid = points.map(lambda p:((float(p[1])//d,float(p[2])//d),p))
#pointsGrid.collect()

#cartesian join on itself
pointsGrid = pointsGrid.cartesian(pointsGrid)
#pointsGrid.collect()

# COMMAND ----------

def filter_neighbor_points(l):
    if ((abs(l[0][0][0] - l[1][0][0]) <=1) & (abs(l[0][0][1] - l[1][0][1]) <=1)) & (l[0][1][0] != l[1][1][0]):
        return (l[0][0], [l[0][1], l[1][1]])
 


filtered_grid = pointsGrid.map(filter_neighbor_points).filter(lambda x: x != None).sortByKey()
#filtered_grid.collect()

# COMMAND ----------

def coord_distance(c):
    x1 = c[1][0][1]
    x2 = c[1][0][2]
    y1 = c[1][1][1]
    y2 = c[1][1][2]
    if math.dist([x1, x2], [y1, y2]) <= d:
        if c[1][0][0] < c[1][1][0]:
            return(c[1][0][0], c[1][1][0])
        else:
            return(c[1][1][0],c[1][0][0])
        
#remove duplicate points       
filtered_grid_coord_distance = filtered_grid.map(coord_distance).filter(lambda x: x != None).distinct().sortByKey()

filtered_grid_coord_distance.collect()

