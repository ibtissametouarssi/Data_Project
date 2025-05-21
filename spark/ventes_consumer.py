from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

# 🔁 Fonctions utilitaires (tu peux les déplacer dans un fichier `common.py` si souhaité)
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
        .appName("KafkaToCassandra-ventes") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()


# 📦 Schéma du topic ventes
schema = StructType([
    StructField("id", StringType(), False),
    StructField("Display Name", StringType(), True),
    StructField("Zone/Display Name", StringType(), True)
])

topic = "ventes"
keyspace = "spark_streams"

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    cluster = Cluster(["localhost"])
    session = cluster.connect()

    create_cassandra_table(session, keyspace, topic, schema)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Renommer les colonnes pour Cassandra
    for old_name in parsed_df.columns:
        new_name = old_name.replace(" ", "_").replace("-", "_").replace("/", "_")
        parsed_df = parsed_df.withColumnRenamed(old_name, new_name)

    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
        .option("keyspace", keyspace) \
        .option("table", topic) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
