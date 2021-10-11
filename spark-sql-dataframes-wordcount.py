from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("data/book.txt")


words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("words"))

wordCounts = lowercaseWords.groupBy("words").count()

#Sort by counts
wordCountsSorted = wordCounts.sort("words")

wordCountsSorted.show(wordCountsSorted.count())

spark.stop()