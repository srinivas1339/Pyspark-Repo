from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col

sparkconf = SparkConf()
sparkconf.set("spark.app.name", "first pyspark")
sparkconf.set("spark.master","local[1]")

spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()

df = spark.read.option("header",True).option("inferSchema",True).csv("C:\\Users\\srinivas\\Desktop\\result.csv")
df=df.select("mail", "phone_no").filter(col("phone_no").like("%119%"))
df.rdd.repartition(5)



