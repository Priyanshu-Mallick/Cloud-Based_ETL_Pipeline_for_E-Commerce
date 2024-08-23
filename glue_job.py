from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parse job name from command-line arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Read data from S3 using Glue Data Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "ecommerce_db",          # Replace with your Glue Data Catalog database name
    table_name = "ecommerce_sales_data" # Replace with your Glue Data Catalog table name
)

# Apply transformations to the data
applymapping = ApplyMapping.apply(
    frame = datasource, 
    mappings = [("order_id", "string", "order_id", "string"), ("total_amount", "double", "total_amount", "double")]
)

# Write transformed data to Amazon Redshift
redshift_sink = glueContext.write_dynamic_frame.from_catalog(
    frame = applymapping, 
    database = "ecommerce_redshift_db",     # Replace with your Redshift database name
    table_name = "transformed_sales_data",  # Replace with your Redshift table name
    redshift_tmp_dir = "s3://ecommerce-redshift-tmp-dir", # Replace with your S3 bucket for temporary data
    transformation_ctx = "redshift_sink"
)

# Commit the Glue job
job.commit()
