# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from decimal import Decimal

# %%
spark = SparkSession.builder.appName('DML').getOrCreate()

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
    [3, 'Vibhooti', 'Divyaraj', Decimal(14)],
    [4, 'Setu', 'Ghosh', Decimal(15)],
    [5, 'Santanu', 'Das', Decimal(16)],
    [6, 'Sujoy', 'Dhar', Decimal(17)]
]

schema_dept = StructType(
    [
        StructField('employee_id', IntegerType(), False),
        StructField('department', StringType(), False)
    ]
)

data_dept = [
    [3, 'IT'],
    [4, 'ACC'],
    [8, 'FIN']
]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

df_dept = spark.createDataFrame(data=data_dept, schema=schema_dept)
df_dept.show()

df.createOrReplaceTempView('emp')
df_dept.createOrReplaceTempView('dept')
# Dataframes are immutable.
# DML on existing dataframe is not possible. 
# New dataframe needs to created with the changes

# %%
# INSERT

# To insert data another dataframe needs to created with same 
# schema --> union with original dataframe -> new dataframe 
# with inserted rows

data_new_recs = [
    [4, 'Rahul', 'Hossain', Decimal(16)]
]
df_new_recs = spark.createDataFrame(data = data_new_recs, schema=schema)

df_insert = df.union(df_new_recs)

df_insert.show()

# %%
# UPDATE
# Modify existing column


# withColumn -> returns a df with all columns
# one column at a time
df_update = df.withColumn('salary', (F.col('salary')*1.05).cast(DecimalType(38, 2)))\
              .withColumn('bonus', (F.col('salary')*.1).cast(DecimalType(38, 2)))
              
df_update.show()


# withColumn -> returns a df with all columns
# multiple columns at a time
df_update = df.withColumns(
    {
        'salary' : (F.col('salary')*1.05).cast(DecimalType(38,2)),
        'bonus' : (F.col('salary')*.1).cast(DecimalType(38, 2))
    }
)

df_update.show()

# %%
# ADD BLANK COLUMN
df_add_col = df.withColumn('new_col', F.lit(None))
df_add_col.show()

# %%
# DROP COLUMN
df_blank_col = df_add_col.drop('new_col')
df_blank_col.show()

# %%
# MERGE

df_left = df.select('employee_id', 'first_name', 'last_name').filter(F.col('employee_id').isin(1, 3, 5))
df_left.show()
df_right = df.select('employee_id', 'salary').filter(F.col('employee_id').isin(2, 3, 4, 6))
df_right.show()

df_left.join(df_right, df_left['employee_id'] == df_right['employee_id'], 'inner').show()

df_left.join(df_right, df_left['employee_id'] == df_right['employee_id'], 'outer').show()

df_left.join(df_right, df_left['employee_id'] == df_right['employee_id'], 'left').show()

df_left.join(df_right, df_left['employee_id'] == df_right['employee_id'], 'left').show()

# %%
# FILTER

df.filter(df['employee_id'] > 3).show()
df.filter(F.col('employee_id').isin(1, 2, 3)).show()

# %%
# TRUNCATE
df_truncate = df.filter(False)
