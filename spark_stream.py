import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.driver.host", "localhost") \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info('Spark connection created successfully!')

        return s_conn
    except Exception as e:
        logging.error(f'Could not create Spark connection: {e}')
        return None


def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users") \
            .option("startingOffsets", "earliest") \
            .load()

        return spark_df
    except Exception as e:
        logging.error(f'Could not connect to kafka: {e}')
        return None


def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'], auth_provider=auth_provider)
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f'Could not create Cassandra connection: {e}')
        return None


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    print('Keyspace created successfully!')


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.users (
        id TEXT PRIMARY KEY,
        gender TEXT,
        first_name TEXT,
        last_name TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT
    );
    """)

    print('Table created successfully!')


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    return spark_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select(
        "data.*")


if __name__ == '__main__':
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        df = create_selection_df_from_kafka(spark_df)

        cas_session = create_cassandra_connection()

        if cas_session is not None:
            create_keyspace(cas_session)
            create_table(cas_session)

            logging.info("Streaming is being started...")
            df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .option("keyspace", "spark_streams") \
                .option("table", "users") \
                .start() \
                .awaitTermination()
