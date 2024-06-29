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

# Script generated for node customer_landing
customer_landing_node1719242779186 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1719242779186")

# Script generated for node Filter for share with research
SqlQuery119 = '''
select * from myDataSource
where sharewithresearchasofdate is not null

'''
Filterforsharewithresearch_node1719242852069 = sparkSqlQuery(glueContext, query = SqlQuery119, mapping = {"myDataSource":customer_landing_node1719242779186}, transformation_ctx = "Filterforsharewithresearch_node1719242852069")

# Script generated for node Customer trusted
Customertrusted_node1719242932504 = glueContext.getSink(path="s3://scottschwarz77-bucket8782/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customertrusted_node1719242932504")
Customertrusted_node1719242932504.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
Customertrusted_node1719242932504.setFormat("json")
Customertrusted_node1719242932504.writeFrame(Filterforsharewithresearch_node1719242852069)
job.commit()