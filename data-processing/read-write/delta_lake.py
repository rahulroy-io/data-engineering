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
    [3, 'Vibhooti', 'Divyaraj', Decimal(14)]
]

df = spark.createDataFrame(data=data, schema=schema)
df.show()