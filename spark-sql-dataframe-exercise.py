from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FakeFriends").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("data/fakefriends-header.csv")

# One liner
# people.groupBy("age").avg("friends").alias("average_friends").show()

# Select only the columns needed
avg_friends = people.select(people.age, people.friends)

print("Group By friends and aggregate the number of friends")
avg_friends.groupBy("age").avg("friends").sort("age").show()

print("Formatted more nicely")
avg_friends.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

#With a custom column name
avg_friends.groupBy("age").agg(func.round(func.avg("friends"),2)).alias("friends_avg").sort("age").show()

spark.stop()