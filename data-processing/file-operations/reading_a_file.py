# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# %%
spark = SparkSession.builder.appName('sqlzoo').getOrCreate()

# %%

# csv
# With Schema
# common options
# header: It indicates whether the first row of the CSV file should be treated as a header or not. Default value is False.
# inferSchema: It instructs Spark to automatically infer the data types of the columns based on the data. Default value is False.
# sep: It is used to specify the delimiter used in the CSV file. Default value is ,.
# quote: It is used to specify the character used to quote strings in the CSV file. Default value is ".
# escape: It is used to specify the escape character used in the CSV file. Default value is \.
# nullValue: It is used to specify the string that denotes a null value in the CSV file. Default value is empty string ''.
# nanValue: It is used to specify the string that denotes a NaN value in the CSV file. Default value is NaN.
# positiveInf: It is used to specify the string that denotes a positive infinity value in the CSV file. Default value is Inf.
# negativeInf: It is used to specify the string that denotes a negative infinity value in the CSV file. Default value is -Inf.
# encoding: It is used to specify the character encoding of the CSV file. Default value is UTF-8.
# maxColumns: It is used to specify the maximum number of columns in the CSV file. Default value is 20480.
# maxCharsPerColumn: It is used to specify the maximum number of characters in each column of the CSV file. Default value is -1, which means there is no limit.
# comment: It is used to specify the character used to indicate a comment in the CSV file. Default value is empty string ''.

schema = StructType([
    StructField('yr', IntegerType(), False),
    StructField('subject', StringType(), False),
    StructField('winner', StringType(), False)
])
df_nobel = spark.read.format('csv')\
                     .options(header=True, inferSchema=False)\
                     .schema(schema)\
                     .load('/home/glue_user/workspace/sparklearning/practice/src_data/nobel/nobel_sqlzoo.csv')

df_nobel.show()
df_nobel.printSchema()


# %%

