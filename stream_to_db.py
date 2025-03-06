from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import os
from dotenv import load_dotenv


load_dotenv(".env")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DB_NAME = os.getenv("DB_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("PASSWORD")

kafka_options = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="";""",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "earliest",
    "subscribe": "events",
    "maxOffsetsPerTrigger": 5000,
}

spark = (
    SparkSession.builder.appName("BlueStreamToDB")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,"
        "software.amazon.msk:aws-msk-iam-auth:1.1.4,"
        "org.postgresql:postgresql:42.5.4",
    )
    .getOrCreate()
)

df = spark.readStream.format("kafka").options(**kafka_options).load()

schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("page", StringType(), True),
        StructField("item_url", StringType(), True),
        StructField("order_email", StringType(), True),
        StructField("channel", StringType(), True),
    ]
)

df = (
    df.select(col("value").cast("string").alias("json_value"))
    .select(from_json("json_value", schema).alias("event"))
    .select("event.*")
)


jdbc_url = f"jdbc:postgresql://{HOST}:{PORT}/{DB_NAME}"

db_properties = {
    "user": POSTGRES_USER,
    "password": PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
}


def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write.jdbc(
            url=jdbc_url,
            table="test_click_data",
            mode="append",
            properties=db_properties,
        )


query = (
    df.writeStream.foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/home/ec2-user/blue/db-checkpoints")
    .start()
)

query.awaitTermination()
