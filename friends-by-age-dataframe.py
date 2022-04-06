from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAgeDataFrame").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

# 필요한 열만 골라 자원 낭비 방지
people = people.select("age", "friends")

people.groupBy("age").avg("friends").show()

people.groupBy("age").avg("friends").sort("age").show()

people.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

people.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()

