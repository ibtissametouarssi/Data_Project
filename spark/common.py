# common.py
import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

def escape_column_name(name):
    return f'"{name}"' if " " in name or "-" in name or "/" in name else name

def create_cassandra_table(session, keyspace, table_name, schema):
    fields = []
    for field in schema.fields:
        cassandra_type = "TEXT"
        col_name = escape_column_name(field.name)
        fields.append(f"{col_name} {cassandra_type}")
    fields[0] += " PRIMARY KEY"
    fields_def = ", ".join(fields)

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
            {fields_def}
        );
    """)
    print(f"✅ Table {table_name} créée dans Cassandra.")

def get_spark_session():
    return SparkSession.builder \
        .appName("KafkaToCassandra") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()
