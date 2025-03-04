# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from decimal import Decimal

# %%
spark = SparkSession.builder\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .appName('DML').getOrCreate()

# %%
schema = StructType(
    [
        StructField('employee_id', IntegerType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('salary', DecimalType(38,2), False)
    ]
)

data = [
    [1, 'Rahul', 'Roy', Decimal(10)],
    [2, 'Pallavi', 'Bhole', Decimal(12)],
    [3, 'Vibhooti', 'Divyaraj', Decimal(14)]
]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

# Dataframes are immutable. 
# DML on existing dataframe is not possible. 
# New dataframe needs to created with the changes

# select -> spark column like operations 
# input -> cols : str, Column, or list
df.select('employee_id', F.col('first_name'), 'last_name', (F.col('salary')*1.05).alias('incremented_salary')).show()

# Using expr we can write sql like syntax inside expr 
# and it returns a column obj
df.select('employee_id', F.col('first_name'), 'last_name', F.expr('salary*1.05 incremented_salary')).show()

# select -> only sql column like operations are allowed
# input -> *expr: str | *expr: List[str]
# expr not supported since it returns column object 
df.selectExpr('employee_id', 'first_name', 'last_name', 'salary*1.05 as incremented_salary').show()


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
df_nobel.createOrReplaceTempView('nobel')
# %%
# SORT

# in pyspark dataframe api both orderby and sort are same 

df_nobel.repartition(10)\
    .sort(F.asc('yr'), F.desc('winner'))\
    .withColumn('partition_id', F.spark_partition_id())\
    .show()

df_nobel.repartition(10)\
    .orderBy(F.asc('yr'), F.desc('winner'))\
    .withColumn('partition_id', F.spark_partition_id())\
    .show()

# sortWithinPartitions sorts within a partition

df_nobel.repartition(10)\
    .sortWithinPartitions(F.asc('yr'), F.desc('winner'))\
    .withColumn('partition_id', F.spark_partition_id())\
    .show()

# orderby vs sort

# %%
df_actor = spark.read.format('csv').option('header', True).load("/home/glue_user/workspace/sparklearning/practice/src_data/actor.csv")
df_booking = spark.read.format('csv').option('header', True).load("/home/glue_user/workspace/sparklearning/practice/src_data/booking.csv")
