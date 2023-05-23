import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

# Get the command-line arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize SparkContext
sc = SparkContext()

# Create GlueContext
glueContext = GlueContext(sc)

# Create SparkSession
spark = glueContext.spark_session

# Create Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from S3 bucket into a DynamicFrame
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://fangyi/customer/landing/"]},
    transformation_ctx="S3bucket_node1",
)

# Apply filter to the DynamicFrame to remove rows where shareWithResearchAsOfDate is 0
ApplyMapping_node2 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping_node2",
)

# Write the filtered DynamicFrame to another S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://fangyi/customer/trusted/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

# Commit the job
job.commit()
