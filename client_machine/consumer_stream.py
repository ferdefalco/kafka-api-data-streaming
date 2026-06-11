import sys
import logging
import json
import re
import argparse
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, instr
from pyspark.sql.types import StructType, StructField, StringType

# configures the logger to display log messages at the INFO level
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# create keyspace in Cassandra
def create_keyspace(session):

    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS user_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )

    print("Keyspace created successfully")


def create_table(session):


    session.execute(
        """
        CREATE TABLE IF NOT EXISTS user_data.tb_users (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """
    )

    print("Table created successfully")

def string_format_cassandra(text: str):
    return re.sub(r"'", r"''", text)


def insert_data(session, row):

    user_id         = string_format_cassandra(row.id)
    first_name      = string_format_cassandra(row.first_name)
    last_name       = string_format_cassandra(row.last_name)
    email           = string_format_cassandra(row.email)
    username        = string_format_cassandra(row.username)
    gender          = string_format_cassandra(row.gender)
    address         = string_format_cassandra(row.address)
    post_code       = string_format_cassandra(row.post_code)
    dob             = string_format_cassandra(row.dob)
    registered_date = string_format_cassandra(row.registered_date)
    phone           = string_format_cassandra(row.phone)
    picture         = string_format_cassandra(row.picture)

    try:
        query = f"""
            INSERT INTO user_data.tb_users(
                id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture
            ) VALUES (
                '{user_id}', '{first_name}', '{last_name}', '{gender}', '{address}', '{post_code}', 
                '{email}', '{username}', '{dob}', '{registered_date}', '{phone}', '{picture}'
            )
        """
        
    
        session.execute(query)
        logging.info(f"Data inserted for: {user_id} - {first_name} {last_name} - {email}")
    except Exception as e:
        logging.error(f"Data could not be inserted due to: {e}")
        print(f"Query:\n{query}")


def create_spark_connection():

    try:
        s_conn = (
            SparkSession.builder.appName("Project1")
            .master("spark://spark-master:7077")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.executor.memory", "1g")
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "2")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark Connection created successfully")
        return s_conn
    except Exception as e:
        logging.error(f"Spark Connection could not be created due to: {e}")
        return None

def create_kafka_connection(spark_conn, stream_mode):

    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "kafka_topic")
            .option("startingOffsets", stream_mode)
            .load()
        )
        logging.info("Dataframe Kafka created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created due to: {e}")
        return None


def create_df_from_kafka(spark_df):

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    return (
        spark_df.selectExpr("CAST(value AS STRING)")            
        .select(from_json(col("value"), schema).alias("data"))  
        .select("data.*")                                       
        .filter(instr(col("email"), "@") > 0)                   
    )


def create_cassandra_connection():

    try:
        cluster = Cluster(["cassandra"])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Cassandra connection could not be created due to: {e}")
        return None


if __name__ == "__main__":
    
    
    parser = argparse.ArgumentParser(description = "Real Time ETL.")
    
    parser.add_argument(
        "--mode",
        required=True,
        help="Data consumption mode",
        choices=["initial", "append"],
        default="append",
    )

    args = parser.parse_args()

    stream_mode = "earliest" if args.mode == "initial" else "latest"

    session = create_cassandra_connection()
    spark_conn = create_spark_connection()

    if session and spark_conn:

        create_keyspace(session)
        create_table(session)

        kafka_df = create_kafka_connection(spark_conn, stream_mode)

        if kafka_df:

            structured_df = create_df_from_kafka(kafka_df)


            def process_batch(batch_df, batch_id):
                
                for row in batch_df.collect():
                    insert_data(session, row)

            query = (
                structured_df.writeStream
                .foreachBatch(process_batch)  
                .start()                      
            )
        
            query.awaitTermination()

