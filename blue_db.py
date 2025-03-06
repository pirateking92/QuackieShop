from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Kafka options – bootstrap server URLs, starting position, and topic name
kafka_options = {
    "kafka.bootstrap.servers": "b-2-public.bluecluster.gc1cio.c3.kafka.eu-west-2.amazonaws.com:9198,b-1-public.bluecluster.gc1cio.c3.kafka.eu-west-2.amazonaws.com:9198",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="";""",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "latest",  # Start from the latest event when we consume from kafka
    "subscribe": "events",  # Our topic name
}

spark = (
    SparkSession.builder.appName("streamProject")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,software.amazon.msk:aws-msk-iam-auth:2.2.0",
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
    ]
)


df = (
    df.select(col("value").cast("string").alias("json_value"))
    .select(from_json("json_value", schema).alias("event"))
    .select("event.*")
)


# 3. The actual processing below!


query = df.writeStream.format("console").start()
query.awaitTermination()
