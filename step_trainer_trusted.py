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

# Script generated for node customer_curated
customer_curated_node1719424433529 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1719424433529")

# Script generated for node step_trainer_landing
step_trainer_landing_node1719335417651 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1719335417651")

# Script generated for node Join tables and select step trainer fields
SqlQuery1410 = '''
select st.sensorreadingtime, st.serialnumber, st.distancefromobject
from st join cc on st.serialnumber = cc.serialnumber
'''
Jointablesandselectsteptrainerfields_node1719348036381 = sparkSqlQuery(glueContext, query = SqlQuery1410, mapping = {"st":step_trainer_landing_node1719335417651, "cc":customer_curated_node1719424433529}, transformation_ctx = "Jointablesandselectsteptrainerfields_node1719348036381")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1719335604851 = glueContext.getSink(path="s3://scottschwarz77-bucket8782/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1719335604851")
step_trainer_trusted_node1719335604851.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1719335604851.setFormat("json")
step_trainer_trusted_node1719335604851.writeFrame(Jointablesandselectsteptrainerfields_node1719348036381)
job.commit()