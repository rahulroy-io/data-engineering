#%%
#imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

#%%
spark = SparkSession.builder.appName('readDataFromDifferentSources').getOrCreate()
##resources##
#https://spark.apache.org/docs/latest/sql-data-sources.html
#%%
#############using list##############
data_list = [
    [1, 'Rahul'],
    [2, 'Pallavi'],
    [3, 'Vibhooti'],
    [4, 'Hossain']
]
schema_list = T.StructType([
    T.StructField('id', T.IntegerType()),
    T.StructField('Name', T.StringType())
])
df_list = spark.createDataFrame(data=data_list, schema=schema_list)
df_list.show()

#%%
#############using dictionary##############
data_json = [
    {"id":1,"Name":"Rahul"},
    {"id":2,"Name":"Pallavi"},
    {"id":3,"Name":"Vibhooti"},
    {"id":4,"Name":"Hossain"}
]
df_json = spark.createDataFrame(data=data_json)
df_json.show()


