from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import monotonically_increasing_id
from cassandra.cluster import Cluster

# 🔧 Fonctions utilitaires
def normalize_column_name(name):
    return (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("'", "")
        .lower()
    )

#  Création automatique de la table Cassandra
def create_cassandra_table(session, keyspace, table_name, schema):
    fields = []
    for field in schema.fields:
        cassandra_type = "TEXT"
        normalized_name = normalize_column_name(field.name)
        fields.append(f"{normalized_name} {cassandra_type}")
    # Ajoute la clé primaire "id" à la fin
    fields.append("id TEXT PRIMARY KEY")
    fields_def = ", ".join(fields)

    # Keyspace
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)

    # Table
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} ({fields_def})
    """)
    print(f"✅ Table {table_name} créée dans Cassandra.")

#  Création session Spark
def get_spark_session():
    return SparkSession.builder \
        .appName("VentesCSVToCassandra") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.ui.port", "4052") \
        .getOrCreate()

#  Schéma du fichier ventes_export.csv (sans id)
schema = StructType([
    StructField("ref", StringType(), True),
    StructField("client", StringType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("facture", StringType(), True),
    StructField("default_code", StringType(), True),
    StructField("produit", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price_unit", StringType(), True),
    StructField("price_subtotal", StringType(), True),
    StructField("categorie", StringType(), True),
    StructField("payment_method", StringType(), True)
])

#  Main
if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    keyspace = "spark_streams"
    table = "ventesexporte"

    # Connexion Cassandra
    cluster = Cluster(["localhost"])
    session = cluster.connect()

    # Création de la table avec id ajouté
    create_cassandra_table(session, keyspace, table, schema)

    #  Lecture du fichier CSV
    df = spark.read.csv(
        "/opt/airflow/dags/files/ventes_export.csv",
        header=True,
        sep=";",
        schema=schema,
        encoding="utf-8"
    )

    # Normalisation correcte des noms de colonnes
    df = df.toDF(*[normalize_column_name(c) for c in df.columns])

    # Ajout d'un ID unique
    df = df.withColumn("id", monotonically_increasing_id().cast("string"))

    # Affichage de contrôle
    print("✅ Colonnes finales dans le DataFrame :", df.columns)

    # Insertion
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .save()

    print("✅ Fichier ventes_export.csv traité avec `id` et inséré dans Cassandra.")
