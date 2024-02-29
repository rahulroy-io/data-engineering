# %%
# imports

from datetime import datetime as dt
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

# %%
# defination

spark = SparkSession.builder.appName("DDL").getOrCreate()


# %%
# TASK

# creating a schema
schema = T.StructType(
    [
        T.StructField('customerid', T.IntegerType(), False),
        T.StructField('amount', T.DecimalType(38,16), False)
    ]
)

# passing data as per schema defined
data = [
    [1, Decimal(10)],
    [2, Decimal(11)]
]

df = spark.createDataFrame(data=data, schema=schema)

df.show()
df.printSchema()

#re-utilize schema
new_schema = T.StructType(df.schema.fields)

#%%
# SCHEMA CHANGES

# adding/renaming/casting a column
df = df.withColumn('qnty', (F.rand()*10).cast('integer'))
df.show()

#dropping column
df = df.drop('qnty')
df.show()
