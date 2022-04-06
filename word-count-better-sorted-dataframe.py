from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words = words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(wordCountsSorted.count())