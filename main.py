from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("first pyspark").getOrCreate()

df = spark.read.option("header",True).option("inferSchema",True).csv("C:\\Users\\srinivas\\Desktop\\result.csv")
df.printSchema()
df.show(10)

