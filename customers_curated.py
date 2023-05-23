from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Get the command-line arguments
args = getResolvedOptions(sys.argv, ['work3'])

# Initialize SparkContext
sc = SparkContext()

# Create GlueContext
glueContext = GlueContext(sc)

# Create SparkSession
spark = glueContext.spark_session

# Define the name of the database and table
database_name = "Database3"
table_name = "customers_curated"

# Define the prefix for the trusted data path
trusted_prefix = "trusted/"

# Define the schema for the customer landing data
customer_landing_schema = StructType([
    StructField("customerName", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("birthDay", StringType(), True),
    StructField("serialNumber", StringType(), True),
    StructField("registrationDate", LongType(), True),
    StructField("lastUpdateDate", LongType(), True),
    StructField("shareWithResearchAsOfDate", LongType(), True),
    StructField("shareWithPublicAsOfDate", LongType(), True)
])

# Define the path for the customer landing data in S3
customer_landing_path = "s3://fangyi/customer/"

# Read the customer landing data into a DataFrame
customer_landing = spark.read.json(customer_landing_path, schema=customer_landing_schema)

# Filter the customer landing data to keep only rows where shareWithResearchAsOfDate and serialNumber are not null
customers_curated = customer_landing.filter((col("shareWithResearchAsOfDate").isNotNull()) & (col("serialNumber").isNotNull()))

# Define the path for the curated customers data in S3
customers_curated_path = "s3://fangyi/" + trusted_prefix + table_name + "/"

# Write the curated customers data to parquet files in S3
customers_curated.write.mode("overwrite").parquet(customers_curated_path)

# Create a DynamicFrame from the parquet files in S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "path": customers_curated_path,
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
print("Job completed: customers_curated was written to " + customers_curated_path)
