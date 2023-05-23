import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext

# Get the command-line arguments
args = getResolvedOptions(sys.argv, ['work5'])

# Initialize SparkContext
sc = SparkContext()

# Create GlueContext
glueContext = GlueContext(sc)

# Create SparkSession
spark = glueContext.spark_session

# Define the name of the database and table
database_name = "database5"
step_trainer_table_name = "step_trainer_trusted"
accelerometer_table_name = "accelerometer_trusted"
machine_learning_table_name = "machine_learning_curated"

# Define the prefix for the trusted data path
trusted_prefix = "trusted/"

# Read step_trainer data from Glue catalog and convert it to DataFrame
step_trainer = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=step_trainer_table_name
).toDF()

# Read accelerometer data from Glue catalog and convert it to DataFrame
accelerometer = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=accelerometer_table_name
).toDF()

# Join step_trainer and accelerometer data based on sensorReadingTime and timeStamp
joined_data = step_trainer.join(
    accelerometer,
    step_trainer.sensorReadingTime == accelerometer.timeStamp,
    "inner"
).select(
    step_trainer.sensorReadingTime,
    step_trainer.serialNumber,
    step_trainer.distanceFromObject,
    accelerometer.x,
    accelerometer.y,
    accelerometer.z
)

# Read customer_data from Glue catalog and convert it to DataFrame
customer_data = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customer_trusted"
).toDF()

# Join joined_data with customer_data based on serialNumber and filter rows with shareWithResearchAsOfDate > 0
curated_data = joined_data.join(
    customer_data,
    joined_data.serialNumber == customer_data.serialNumber,
    "inner"
).filter(
    col("shareWithResearchAsOfDate") > 0
)

# Define the path for the curated_data in S3
curated_data_path = "s3://fangyi/" + trusted_prefix + machine_learning_table_name + "/"

# Write the curated_data to parquet files in S3
curated_data.write.mode("overwrite").parquet(curated_data_path)

# Create a DynamicFrame from the parquet files in S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "path": curated_data_path,
        "partitionKeys": []
    },
    format="parquet"
)

# Convert the DynamicFrame to a DataFrame and save it as a table in Glue catalog
dynamic_frame.toDF().write.saveAsTable(
    name=machine_learning_table_name,
    format="parquet",
    mode="overwrite",
    partitionBy=[]
)

# Print the completion message
print("Job completed: machine_learning_curated was written to " + curated_data_path)
