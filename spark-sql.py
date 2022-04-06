from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode('utf-8')),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

# 스키마를 추론하고, 데이터프레임을 테이블로 등록
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
