# Amazon Web Services (AWS) Intro
I wanted to explore AWS to prepare for a Data Engineer interview with KBC.
I decided to do a little project, where I implement an ETL pipeline with Spark and run it on AWS.
I followed this tutorial - https://www.datacamp.com/tutorial/aws-glue
For this exercise I will store some dataset in an S3 bucket and run an ETL pipeline to process it and output it back into the S3 bucket.
I have 2 options for running the ETL pyspark script:
1. Glue - serverless, more limited, focus on simple ETL
1. EMR (elastic mapreduce) - basically an EC2 instance preconfigured for running pyspark + hadoop - more freedom, but also more cost

## Setup AWS and run ETL jobs

1. Created AWS account
1. Go To IAM -> Roles -> Create Role
    * Usecase Glue, Add Policies: `AWSGlueServiceRole`, `AmazonS3FullAccess`
1. Create S3 Bucket
1. Upload data to input folder in S3 bucket
1. In Glue - create a crawler, run crawler on the S3 bucket
    * discovers and catalogues data automatically
    * `crawl all sub-folders` has to be enabled
1. In glue - create an ETL job
    * start fresh - use spark/python engine
    * in editor - write our pyspark code (check python files in this repo), note the peculiarities of using sparkcontext/sparksession and glue!
    * in job details - fill out the IAM role, set job type to spark, reduce number of executors and maximum runtime to protect your budget
    * run!
    * check S3 output folder
1. Destroy all resources you used to prevent further costs


