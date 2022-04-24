#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round
from pyspark.sql import functions as F
import pyspark
import pandas as pd

# http://netsg.cs.sfu.ca/youtubedata/

# Count videos in categories
# Determine average viewership based on categories
# Determine top ~twenty videos based on view {and frequency of categories in this list}
# Determine bottom ~twenty videos based on view {and frequency of categories in this list}
# Compare previous two

def main():
    pySparkSes = SparkSession.builder.getOrCreate()

    yt0518_0 = pySparkSes.read.csv("src/0518/0.txt", sep = "\t")
    yt0518_1 = pySparkSes.read.csv("src/0518/1.txt", sep = "\t")
    yt0518_2 = pySparkSes.read.csv("src/0518/2.txt", sep = "\t")
    yt0518_3 = pySparkSes.read.csv("src/0518/3.txt", sep = "\t")

    yt0518_0 = yt0518_0.withColumnRenamed('_c0', "ID").withColumnRenamed('_c1', "Uploader")
    yt0518_0 = yt0518_0.withColumnRenamed('_c2', "Age").withColumnRenamed('_c3', "Category")
    yt0518_0 = yt0518_0.withColumnRenamed('_c4', "Length").withColumnRenamed('_c5', "Views")
    yt0518_0 = yt0518_0.withColumnRenamed('_c6', "Rate").withColumnRenamed('_c7', "Ratings")
    yt0518_0 = yt0518_0.withColumnRenamed('_c8', "Comments").withColumnRenamed('_c9', "Related")
    yt0518_0 = yt0518_0.drop(*[str(x) for x in yt0518_0.columns[9:-1]])

    yt0518_1 = yt0518_1.withColumnRenamed('_c0', "ID").withColumnRenamed('_c1', "Uploader")
    yt0518_1 = yt0518_1.withColumnRenamed('_c2', "Age").withColumnRenamed('_c3', "Category")
    yt0518_1 = yt0518_1.withColumnRenamed('_c4', "Length").withColumnRenamed('_c5', "Views")
    yt0518_1 = yt0518_1.withColumnRenamed('_c6', "Rate").withColumnRenamed('_c7', "Ratings")
    yt0518_1 = yt0518_1.withColumnRenamed('_c8', "Comments").withColumnRenamed('_c9', "Related")
    yt0518_1 = yt0518_1.drop(*[str(x) for x in yt0518_1.columns[9:-1]])

    yt0518_2 = yt0518_2.withColumnRenamed('_c0', "ID").withColumnRenamed('_c1', "Uploader")
    yt0518_2 = yt0518_2.withColumnRenamed('_c2', "Age").withColumnRenamed('_c3', "Category")
    yt0518_2 = yt0518_2.withColumnRenamed('_c4', "Length").withColumnRenamed('_c5', "Views")
    yt0518_2 = yt0518_2.withColumnRenamed('_c6', "Rate").withColumnRenamed('_c7', "Ratings")
    yt0518_2 = yt0518_2.withColumnRenamed('_c8', "Comments").withColumnRenamed('_c9', "Related")
    yt0518_2 = yt0518_2.drop(*[str(x) for x in yt0518_2.columns[9:-1]])

    yt0518_3 = yt0518_3.withColumnRenamed('_c0', "ID").withColumnRenamed('_c1', "Uploader")
    yt0518_3 = yt0518_3.withColumnRenamed('_c2', "Age").withColumnRenamed('_c3', "Category")
    yt0518_3 = yt0518_3.withColumnRenamed('_c4', "Length").withColumnRenamed('_c5', "Views")
    yt0518_3 = yt0518_3.withColumnRenamed('_c6', "Rate").withColumnRenamed('_c7', "Ratings")
    yt0518_3 = yt0518_3.withColumnRenamed('_c8', "Comments").withColumnRenamed('_c9', "Related")
    yt0518_3 = yt0518_3.drop(*[str(x) for x in yt0518_3.columns[9:-1]])

    ytUnion = yt0518_0.union(yt0518_1).union(yt0518_2).union(yt0518_3)

    ############################################
    # Graph of relevant data averages
    ############################################

    print("Graph of relevant data averages:")
    graph0518_avgs = ytUnion.select('Category', 'Views', 'Rate', 'Ratings', 'Comments')
    graph0518_avgs.groupBy('Category').agg(round(F.mean('Views'), 2), round(F.mean('Rate'), 2), round(F.mean('Ratings'), 2), round(F.mean('Comments'), 2), F.count('Category')).where(graph0518_avgs.Category != 'null').sort(col('count(Category)').desc()).show()

    ############################################
    # Count per category
    ############################################

    print("Graph of count per category:")
    graph0518_cnt = ytUnion.select('Category')
    graph0518_cnt.groupBy('Category').count().where(graph0518_cnt.Category != 'null').sort(col('count').desc()).show()

    ############################################
    # Average viewership
    ############################################

    print("Graph of average viewship:")
    graph0518_vws = ytUnion.select('Category', 'Views')
    graph0518_vws.groupBy('Category').agg(round(F.mean('Views'), 2), F.count('Category')).where(graph0518_vws.Category != 'null').sort(col('round(avg(Views), 2)').desc()).show()

    ############################################
    # Top twenty based on views
    ############################################

    # NOT PRINTING THE NUMBER OF VIEWS CORRECTLY
    
    #print("Graph of top 20 videos by view count:")
    #graph0518_top = ytUnion.select('Category', 'Views', 'Rate', 'Ratings', 'Comments')
    #graph0518_top.sort(F.desc('Views')).show()

    ############################################
    # Bottom twenty based on views
    ############################################

    print("Graph of bottom 40 videos by view count:")
    graph0518_bot = ytUnion.select('Category', 'Views', 'Rate', 'Ratings', 'Comments')
    graph0518_bot.where(graph0518_bot.Category != 'null').orderBy(F.asc('Views')).show(40)


if __name__ == "__main__":
    main()