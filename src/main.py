#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round
from pyspark.sql import functions as F
import pyspark
import pandas as pd

# http://netsg.cs.sfu.ca/youtubedata/

def main():
    pySparkSes = SparkSession.builder.getOrCreate()

    yt0518_0 = pySparkSes.read.csv("src/0518/0.txt", sep = "\t")
    #yt0518_1 = pySparkSes.read.csv("src/0518/1.txt", sep = "\t")
    #yt0518_2 = pySparkSes.read.csv("src/0518/2.txt", sep = "\t")
    #yt0518_3 = pySparkSes.read.csv("src/0518/3.txt", sep = "\t")

    yt0518_0 = yt0518_0.withColumnRenamed('_c0', "ID").withColumnRenamed('_c1', "Uploader")
    yt0518_0 = yt0518_0.withColumnRenamed('_c2', "Age").withColumnRenamed('_c3', "Category")
    yt0518_0 = yt0518_0.withColumnRenamed('_c4', "Length").withColumnRenamed('_c5', "Views")
    yt0518_0 = yt0518_0.withColumnRenamed('_c6', "Rate").withColumnRenamed('_c7', "Ratings")
    yt0518_0 = yt0518_0.withColumnRenamed('_c8', "Comments").withColumnRenamed('_c9', "Related")
    yt0518_0 = yt0518_0.drop(*[str(x) for x in yt0518_0.columns[9:-1]])


    graph0518 = yt0518_0.select('Category', 'Views', 'Rate', 'Ratings', 'Comments')

    graph0518.groupBy('Category').agg(round(F.mean('Views'), 2), round(F.mean('Rate'), 2), round(F.mean('Ratings'), 2), round(F.mean('Comments'), 2), F.count('Category')).show()

    #graph0518 = graph0518.groupBy('Category')
    #graph0518.avg('Views')#.agg('Rate').agg('Ratings').agg('Comments')

    #graph0518.show(10)

if __name__ == "__main__":
    main()