import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3://warehouse/"
STORAGE_URI = "http://172.18.0.5:9000" # update minio ip with cmd docker inspect minio

conf = (
    pyspark.SparkConf()
        .setAppName('sales_data_app')
        .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Session Started")

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

sales_data = [
    (1, 101, "Laptop", 1, 1000.00, "2023-08-01"),
    (2, 102, "Mouse", 2, 25.50, "2023-08-01"),
    (3, 103, "Keyboard", 1, 45.00, "2023-08-01"),
    (1, 101, "Laptop", 1, 1000.00, "2023-08-01"),  # Duplicate
    (4, 104, "Monitor", None, 200.00, "2023-08-02"),  # Missing quantity
    (5, None, "Mouse", 1, 25.50, "2023-08-02")  # Missing customer_id
]

sales_df = spark.createDataFrame(sales_data, schema)

spark.sql("CREATE NAMESPACE nessie.sales;").show()

sales_df.writeTo("nessie.sales.sales_data_raw").createOrReplace()

spark.read.table("nessie.sales.sales_data_raw").show()

spark.stop()