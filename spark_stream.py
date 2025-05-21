import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# 📦 Définition des schémas des topics Kafka
schemas = {
    "users_created": StructType([
        StructField("Code unique", StringType(), False),
        StructField("Nom", StringType(), False),
        StructField("Catégorie interne", StringType(), False),
        StructField("Nom2", StringType(), False),
        StructField("Gratuit", StringType(), False),
        StructField("Secteur", StringType(), False),
        StructField("La Zone", StringType(), False),
        StructField("Secteur2", StringType(), False),
        StructField("Adresse complète", StringType(), True),
        StructField("Rue", StringType(), False),
        StructField("Ville", StringType(), False),
        StructField("Contrat signé", StringType(), False)
    ]),
    "ventes": StructType([
        StructField("id", StringType(), False),
        StructField("Display Name", StringType(), True),
        StructField("Zone/Display Name", StringType(), True)
    ]),
    "produits": StructType([
        StructField("id", StringType(), False),
        StructField("Nom d'affichage", StringType(), True),
        StructField("PU HT", StringType(), True),
        StructField("PU TTC", StringType(), True)
    ])
}

# 🔧 Échappe les noms de colonnes avec des espaces ou caractères spéciaux pour Cassandra
def escape_column_name(name):
    return f'"{name}"' if " " in name or "-" in name or "/" in name else name

# 🧱 Crée dynamiquement une table Cassandra
def create_cassandra_table(session, keyspace, table_name, schema):
    fields = []
    for field in schema.fields:
        cassandra_type = "TEXT"  # Tous les champs sont TEXT ici
        col_name = escape_column_name(field.name)
        fields.append(f"{col_name} {cassandra_type}")

    fields[0] += " PRIMARY KEY"  # 1er champ = clé primaire
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

    print(f"✅ Table {table_name} créée avec succès dans le keyspace {keyspace}")

# 🚀 Crée une session Spark
def get_spark_session():
    return SparkSession.builder \
        .appName("GenericKafkaToCassandra") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()

# 🔄 Lit depuis un topic Kafka et écrit dans Cassandra
def stream_topic_to_cassandra(spark, topic, schema, keyspace):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 🔁 Renomme les colonnes dans Spark pour éviter les erreurs (espaces, /, etc.)
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

    return query

# 🧠 Programme principal
if __name__ == "__main__":
    keyspace = "spark_streams"
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # 📡 Connexion Cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect()

    queries = []

    # Pour chaque topic → créer la table Cassandra + lancer le streaming Spark
    for topic, schema in schemas.items():
        create_cassandra_table(session, keyspace, topic, schema)
        q = stream_topic_to_cassandra(spark, topic, schema, keyspace)
        queries.append(q)

    # ⏳ Attente de tous les streamings
    for q in queries:
        q.awaitTermination()
