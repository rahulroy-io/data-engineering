#%%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

#%%
spark = SparkSession.builder.appName('readwrite').getOrCreate()

#%% https://spark.apache.org/docs/latest/sql-data-sources.html
#csv
df_csv = spark.read.format('csv').options(header='true').load('/home/glue_user/workspace/sparklearning/src_data_csv/Customer.csv')
df_csv.printSchema()

#%%parquet
df_parquet = spark.read.format('parquet').load('/home/glue_user/workspace/sparklearning/src_data_pq')

#%%JDBC
#a server can contain multiple databases->schema->tablename
df_jdbc = spark.read.format('jdbc').options(url='jdbc:postgresql://something.com:potnumber/database', 
                                            user='user', 
                                            password='password', 
                                            dbtable='schema.table_name')
query = '''
select
    *
from
    schema.tablename'''
df_jdbc = spark.read.format('jdbc').options(url='jdbc:postgresql://something.com:potnumber/database', 
                                            user='user', 
                                            password='password', 
                                            query=query)
#%%
#json
df_json = spark.read.format('json').load('/home/glue_user/workspace/sparklearning/src_data_json/sample')

#%%
spark.stop()