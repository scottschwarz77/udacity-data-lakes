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
accelerometer_trusted_node1719348907793 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719348907793")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1719348863885 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1719348863885")

# Script generated for node SQL Query
SqlQuery1496 = '''
select * from at
join st on at.timestamp = st.sensorreadingtime

'''
SQLQuery_node1719348945486 = sparkSqlQuery(glueContext, query = SqlQuery1496, mapping = {"at":accelerometer_trusted_node1719348907793, "st":step_trainer_trusted_node1719348863885}, transformation_ctx = "SQLQuery_node1719348945486")

# Script generated for node machine_learning_curated
machine_learning_curated_node1719349122718 = glueContext.getSink(path="s3://scottschwarz77-bucket8782/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1719349122718")
machine_learning_curated_node1719349122718.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1719349122718.setFormat("json")
machine_learning_curated_node1719349122718.writeFrame(SQLQuery_node1719348945486)
job.commit()