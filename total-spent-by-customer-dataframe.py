from ast import Try
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpent").master("local[*]").getOrCreate()

schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("productID", IntegerType(), True),
    StructField("price", FloatType(), True)
])

df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

customerPrice = df.select("customerID", "price")
totalPrice = customerPrice.groupBy("customerID").sum("price").withColumn(
    "totalPrice", func.round(func.col("sum(price)"), 2)).select("customerID", "totalPrice").sort("totalPrice")
totalPrice.show(totalPrice.count())
