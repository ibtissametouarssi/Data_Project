from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

# Fonction pour échapper les noms de colonnes avec des caractères spéciaux
def escape_column_name(name):
    return f'"{name}"' if " " in name or "-" in name or "/" in name else name

# Fonction pour normaliser les noms de colonnes (pour Cassandra)
def normalize_column_name(name):
    return name.replace(" ", "_").replace("-", "_").replace("/", "_").lower()

# Création de la table Cassandra avec le bon schéma
def create_cassandra_table(session, keyspace, table_name, schema):
    fields = []
    for field in schema.fields:
        cassandra_type = "TEXT"
        col_name = escape_column_name(normalize_column_name(field.name))
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

# Spark Session configurée avec les bons packages
def get_spark_session():
    return SparkSession.builder \
        .appName("KafkaToCassandra-ventes") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "localhost") \
        .getOrCreate()

# Schéma des données JSON du topic ventes
schema = StructType([
    StructField("id", StringType(), False),
    StructField("Display Name", StringType(), True),
    StructField("Zone/Display Name", StringType(), True)
])

topic = "ventes"
keyspace = "spark_streams"
table = topic  # même nom pour le topic et la table

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Connexion Cassandra
    cluster = Cluster(["localhost"])
    session = cluster.connect()

    # Création de la table Cassandra
    create_cassandra_table(session, keyspace, table, schema)

    # Lecture du topic Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse des messages JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Renommage des colonnes pour Cassandra
    for old_name in parsed_df.columns:
        new_name = normalize_column_name(old_name)
        parsed_df = parsed_df.withColumnRenamed(old_name, new_name)

    # Supprimer les lignes sans clé primaire
    parsed_df = parsed_df.filter(col("id").isNotNull())

    # Envoi vers Cassandra
    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
