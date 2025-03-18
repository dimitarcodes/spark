from pyspark.sql import (
    SparkSession,
    types,
    functions as F,
)

# initialize spark session
spark = (
    SparkSession
    .builder
    .appName('dataset_cleanup')
    .getOrCreate()
)

# read data
orders_data = spark.read.parquet('data/orders_data.parquet')

# clean up data

# time of day column
orders_data = orders_data.withColumn("time_of_day", 
        F.when( ( F.hour(F.col('order_date')) >= 5) & ( F.hour(F.col('order_date')) < 12), 'morning' ) \
        .when( ( F.hour(F.col('order_date')) >= 12) & ( F.hour(F.col('order_date')) < 18), 'afternoon' ) \
        .when( ( F.hour(F.col('order_date')) >= 18) & ( F.hour(F.col('order_date')) < 24), 'evening' )
        .otherwise( None ))
# filter out Nones -> made between midnight and 5am
orders_data = orders_data.filter(~orders_data['time_of_day'].isNull())

# cast to DateType
orders_data = orders_data.withColumn("order_date", F.col("order_date").cast(types.DateType()))

# lower case the category column
orders_data = orders_data.withColumn("category", F.lower(orders_data["category"])) 

# lower case the product column
orders_data = orders_data.withColumn("product", F.lower(orders_data["product"])) 

# filter out rows with 'tv' in the product column
orders_data = orders_data.filter(~F.col('product').contains("tv")) 

# create a state column based on purchase address
orders_data = orders_data.withColumn("purchase_state", F.substring(F.col("purchase_address"), -8,2))

# write out cleaned dataset
orders_data.write.parquet('data/orders_data_clean.parquet', mode='overwrite')