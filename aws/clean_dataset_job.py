import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import (types, functions as F)
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set Spark configurations for optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


input_path = "s3://glue-etl-demo-dmtr/input/orders_data.parquet"
output_path = "s3://glue-etl-demo-dmtr/output/orders_data_processed.parquet"

# read data
orders_data = spark.read.parquet(input_path)

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
orders_data.write.parquet(output_path, mode='overwrite')

job.commit()
print("job completed")