import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
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

# Set the input and output paths
input_path = "s3://glue-etl-demo-dmtr/input/"
output_path = "s3://glue-etl-demo-dmtr/output/"

# Function to read CSV and write Parquet
def csv_to_parquet(input_file, output_file):
    try:
        # Read CSV
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .option("mode", "PERMISSIVE") \
                       .option("columnNameOfCorruptRecord", "_corrupt_record") \
                       .csv(input_file)
        
        # Print schema for debugging
        print(f"Schema for {input_file}:")
        df.printSchema()
        
        # Convert date column if it exists (assuming it's in the format M/d/yyyy)
        if "date" in df.columns:
            df = df.withColumn("date", to_date(col("date"), "M/d/yyyy"))
        
        # Write Parquet
        df.write.mode("overwrite").parquet(output_file)
        
        print(f"Successfully converted {input_file} to Parquet at {output_file}")
        print(f"Number of rows processed: {df.count()}")
    except Exception as e:
        print(f"Error processing {input_file}: {str(e)}")

# List of files to process
files = ["athletes", "events", "medals"]

# Process each file
for file in files:
    input_file = f"{input_path}{file}.csv"
    output_file = f"{output_path}{file}_parquet"
    csv_to_parquet(input_file, output_file)

job.commit()
print("Job completed.")