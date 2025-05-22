from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster
from unidecode import unidecode

# Échappe les noms de colonnes pour Cassandra
def escape_column_name(name):
    return f'"{name}"' if " " in name or "-" in name or "/" in name else name

# Crée dynamiquement une table Cassandra avec un champ ID unique comme clé primaire

def clean_column_name(name):
    name_ascii = unidecode(name)  # remplace les accents par leur équivalent ASCII
    name_ascii = name_ascii.replace(" ", "_").replace("-", "_").replace("/", "_")
    return name_ascii.lower()  # optionnel : tout en minuscules pour uniformité

def create_cassandra_table(session, keyspace, table_name, schema):
    fields = ["id TEXT PRIMARY KEY"]
    for field in schema.fields:
        col_name = clean_column_name(field.name)
        fields.append(f"{col_name} TEXT")
    fields_def = ",\n    ".join(fields)

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
            {fields_def}
        );
    """
    session.execute(create_table_query)
    print(f"✅ Table `{table_name}` créée dans le keyspace `{keyspace}`.")

# Crée une session Spark
def get_spark_session():
    return SparkSession.builder \
        .appName("KafkaToCassandra-les-ventes") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.ui.port", "4051") \
        .getOrCreate()

# Schéma du topic les ventes
schema = StructType([
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
])

topic = "les_ventes"
keyspace = "spark_streams"
table = topic

# Programme principal
if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    cluster = Cluster(["localhost"])
    session = cluster.connect()

    # Créer table Cassandra avec champ id
    create_cassandra_table(session, keyspace, table, schema)

    # Lecture du stream Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parsing JSON + nettoyage noms colonnes
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    for old_name in parsed_df.columns:
        new_name = old_name.replace(" ", "_").replace("-", "_").replace("/", "_")
        parsed_df = parsed_df.withColumnRenamed(old_name, new_name)

    # Ajout d'un ID unique
    parsed_df = parsed_df.withColumn("id", expr("uuid()"))

    # Insertion dans Cassandra
    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
