import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1719250467750 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1719250467750")

# Script generated for node accelerometer_landing
accelerometer_landing_node1719250507066 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1719250507066")

# Script generated for node Join
Join_node1719250561580 = Join.apply(frame1=accelerometer_landing_node1719250507066, frame2=customer_trusted_node1719250467750, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1719250561580")

# Script generated for node Select Fields
SelectFields_node1719310751438 = SelectFields.apply(frame=Join_node1719250561580, paths=["z", "y", "x", "user", "timestamp"], transformation_ctx="SelectFields_node1719310751438")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719250752960 = glueContext.getSink(path="s3://scottschwarz77-bucket8782/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1719250752960")
accelerometer_trusted_node1719250752960.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1719250752960.setFormat("json")
accelerometer_trusted_node1719250752960.writeFrame(SelectFields_node1719310751438)
job.commit()