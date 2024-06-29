import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719322238782 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719322238782")

# Script generated for node customers_trusted
customers_trusted_node1719321913657 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customers_trusted_node1719321913657")

# Script generated for node Select customer fields
SqlQuery1173 = '''
select distinct serialnumber,sharewithpublicasofdate, birthday,
registrationdate, sharewithresearchasofdate,
customername, email, lastupdatedate, phone,
sharewithfriendsasofdate from at
join ct on at.user = ct.email
'''
Selectcustomerfields_node1719329795602 = sparkSqlQuery(glueContext, query = SqlQuery1173, mapping = {"at":accelerometer_trusted_node1719322238782, "ct":customers_trusted_node1719321913657}, transformation_ctx = "Selectcustomerfields_node1719329795602")

# Script generated for node customer_curated
customer_curated_node1719322499579 = glueContext.getSink(path="s3://scottschwarz77-bucket8782/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1719322499579")
customer_curated_node1719322499579.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1719322499579.setFormat("json")
customer_curated_node1719322499579.writeFrame(Selectcustomerfields_node1719329795602)
job.commit()