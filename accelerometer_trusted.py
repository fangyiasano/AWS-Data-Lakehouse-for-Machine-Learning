import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Get the command-line arguments
args = getResolvedOptions(sys.argv, ['work2'])

# Initialize SparkContext
sc = SparkContext()

# Create GlueContext
glueContext = GlueContext(sc)

# Create SparkSession
spark = glueContext.spark_session

# Define the name of the database and table
database_name = "Database2"
table_name = "accelerometer_trusted"

# Define the prefix for the trusted data path
trusted_prefix = "trusted/"

# Define the schema for the accelerometer landing data
accelerometer_landing_schema = StructType([
    StructField("user", StringType(), True),
    StructField("timeStamp", LongType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", DoubleType(), True)
])

# Define the path for the accelerometer landing data in S3
accelerometer_landing_path = "s3://fangyi/accelerometer/"

# Read the accelerometer landing data into a DataFrame
accelerometer_landing = spark.read.json(accelerometer_landing_path, schema=accelerometer_landing_schema)

# Read the customer_trusted data from the Glue catalog and convert it to a DataFrame
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customer_trusted"
).toDF()

# Join the accelerometer landing data with the customer_trusted data
# based on the user email and check if the shareWithResearchAsOfDate is not null
accelerometer_trusted = accelerometer_landing.join(
    customer_trusted,
    (accelerometer_landing.user == customer_trusted.email) & (customer_landing.shareWithResearchAsOfDate.isNotNull()),
    "inner"
).select(
    accelerometer_landing.user,
    accelerometer_landing.timeStamp,
    accelerometer_landing.x,
    accelerometer_landing.y,
    accelerometer_landing.z
)

# Define the path for the trusted accelerometer data in S3
accelerometer_trusted_path = "s3://fangyi/" + trusted_prefix + table_name + "/"

# Write the trusted accelerometer data to parquet files in S3
accelerometer_trusted.write.mode("overwrite").parquet(accelerometer_trusted_path)

# Create a DynamicFrame from the parquet files in S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "path": accelerometer_trusted_path,
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
print("Job completed: accelerometer_trusted was written to " + accelerometer_trusted_path)
