# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pivot').getOrCreate()

#%%
data = [
    ["Product A", "Region 1", 100, "Category X", "2019-01-01"],
    ["Product A", "Region 2", 200, "Category Y", "2019-01-02"],
    ["Product B", "Region 1", 300, "Category X", "2019-01-03"],
    ["Product B", "Region 2", 400, "Category Y", "2019-01-04"],
    ["Product C", "Region 1", 500, "Category X", "2019-01-05"],
    ["Product C", "Region 2", 600, "Category Y", "2019-01-06"],
    ["Product D", "Region 1", 700, "Category X", "2019-01-07"],
    ["Product D", "Region 2", 800, "Category Y", "2019-01-08"]
]

df = spark.createDataFrame(data=data, schema=["Product", "Region", "Amount", "Category", "Date"])

# %%

df.show()