from pyspark.sql import SparkSession
from pyspark.sql import types as T, functions as F

spark = SparkSession.builder.appName("practice").getOrCreate()
sc = spark.sparkContext

df = spark.createDataFrame([("US",), ("IN",), ("CA",), ("FR",)], ["CountryCode"])
df.show()

import time
time.sleep(300)