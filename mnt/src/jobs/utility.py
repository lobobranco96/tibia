import pyspark
from pyspark.sql import SparkSession
import os

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
NESSIE_URI = os.getenv("NESSIE_URI")


def create_spark_session(appname):
  master = "spark://spark-master:7077"

  conf = (
      pyspark.SparkConf()
      .setAppName(appname)
      .set("spark.master", master)
      .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
      .set("spark.executor.memory", "2g")
      .set("spark.executor.cores", "2")
      .set("spark.executor.instances", "1")
      .set("spark.driver.memory", "2g")
      .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.nessie.s3.path-style-access", "true")
      .set("spark.sql.catalog.nessie.s3.endpoint", S3_ENDPOINT)
      .set("spark.sql.catalog.nessie.warehouse", "s3a://bronze/")
      .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
      .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
      .set("spark.sql.catalog.nessie.ref", "main")
      .set("spark.sql.catalog.nessie.authentication.type", "NONE")
      .set("spark.sql.catalog.nessie.cache-enabled", "false")
      .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
      .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
      .set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  )

  spark = SparkSession.builder.config(conf=conf).getOrCreate()


  return spark
