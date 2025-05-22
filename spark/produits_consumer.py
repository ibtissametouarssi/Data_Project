from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster

# Utilitaire pour échapper les noms de colonnes
def escape_column_name(name):
    return f'"{name}"' if " " in name or "-" in name or "/" in name else name

# Utilitaire pour normaliser les noms de colonnes
def normalize_column_name(name):
    return (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("'", "")
        .lower()
    )

# Création automatique de la table Cassandra
def create_cassandra_table(session, keyspace, table_name, schema):
    fields = []
    for field in schema.fields:
        cassandra_type = "TEXT"
        # Normalise le nom AVANT d'ajouter le champ
        normalized_name = normalize_column_name(field.name)
        fields.append(f"{normalized_name} {cassandra_type}")
    fields[0] += " PRIMARY KEY"
    fields_def = ", ".join(fields)

    # Création du keyspace
    create_keyspace_query = (
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
    )
    session.execute(create_keyspace_query)

    # Création de la table
    create_table_query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} ({fields_def});"
    print("Requête CQL exécutée pour la table :")
    print(create_table_query)
    session.execute(create_table_query)
    print(f"✅ Table {table_name} créée dans Cassandra.")


# Configuration de la session Spark
def get_spark_session():
    return SparkSession.builder \
        .appName("KafkaToCassandra-produits") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

# Schéma JSON des données du topic "produits"
schema = StructType([
    StructField("id", StringType(), False),
    StructField("Nom d'affichage", StringType(), True),
    StructField("PU HT", StringType(), True),
    StructField("PU TTC", StringType(), True)
])

topic = "produits"
keyspace = "spark_streams"
table = topic

# Main
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

    # Parsing JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Renommage des colonnes pour Cassandra
    for old_name in parsed_df.columns:
        new_name = normalize_column_name(old_name)
        parsed_df = parsed_df.withColumnRenamed(old_name, new_name)

    # Ignorer les lignes avec clé primaire manquante
    parsed_df = parsed_df.filter(col("id").isNotNull())

    # Écriture vers Cassandra
    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
