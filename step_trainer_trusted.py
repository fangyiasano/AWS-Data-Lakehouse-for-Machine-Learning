import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame

# Get the command-line arguments
args = getResolvedOptions(sys.argv, ['work4'])

# Initialize SparkContext
sc = SparkContext()

# Create GlueContext
glueContext = GlueContext(sc)

# Create SparkSession
spark = glueContext.spark_session

# Define the name of the database and table
database_name = "database4"
table_name = "step_trainer_trusted"

# Define the prefix for the trusted data path
trusted_prefix = "trusted/"

# Define the schema for the step_trainer data
step_trainer_schema = StructType([
    StructField("sensorReadingTime", LongType(), True),
    StructField("serialNumber", StringType(), True),
    StructField("distanceFromObject", IntegerType(), True)
])

# Define the path for the step_trainer data in S3
step_trainer_path = "s3://fangyi/step_trainer/"

# Read the step_trainer data into a DataFrame
step_trainer = spark.read.json(step_trainer_path, schema=step_trainer_schema)

# Read the customer_trusted data from the Glue catalog and convert it to a DataFrame
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customers_curated"
).toDF()

# Join the step_trainer data with the customer_trusted data based on serialNumber
step_trainer_trusted = step_trainer.join(
    customer_trusted,
    step_trainer.serialNumber == customer_trusted.serialNumber,
    "inner"
).select(
    step_trainer.sensorReadingTime,
    step_trainer.serialNumber,
    step_trainer.distanceFromObject
)

# Define the path for the trusted step_trainer data in S3
step_trainer_trusted_path = "s3://fangyi/" + trusted_prefix + table_name + "/"

# Write the trusted step_trainer data to parquet files in S3
step_trainer_trusted.write.mode("overwrite").parquet(step_trainer_trusted_path)

# Create a DynamicFrame from the parquet files in S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "path": step_trainer_trusted_path,
        "partitionKeys": []
    },
    format="parquet"
)

# Convert the DynamicFrame to a DataFrame and save it as a table in Glue catalog
dynamic_frame.toDF().write.saveAsTable(
    name=table_name,
    format="parquet",
    mode="overwrite",
    partitionBy=[]
)

# Print the completion message
print("Job completed: step_trainer_trusted was written to " + step_trainer_trusted_path)
