import os
import json
import logging
import pandas as pd
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 0)
}

# Répertoire contenant les fichiers à streamer
DIRECTORY_PATH = '/opt/airflow/dags/files/'

def stream_all_files_to_kafka(directory_path):
    logging.info(f"🚀 Début du streaming depuis le dossier : {directory_path}")

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
    )

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        if not os.path.isfile(file_path):
            continue

        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig', on_bad_lines='skip')
            elif filename.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            else:
                logging.warning(f"⚠️ Type de fichier non supporté : {filename}")
                continue

            topic = os.path.splitext(filename)[0]
            logging.info(f"📄 Fichier détecté : {filename} ➤ Topic Kafka : {topic}")

            for _, row in df.iterrows():
                try:
                    message = row.dropna().to_dict()
                    clean_message = json.loads(json.dumps(message, default=str))  # nettoie les types spéciaux
                    producer.send(topic, clean_message)
                    logging.debug(f"✔ Message envoyé à {topic}: {clean_message}")
                except Exception as e:
                    logging.error(f"❌ Erreur d'envoi au topic '{topic}': {e}\nContenu de la ligne: {row}")

            logging.info(f"✅ {len(df)} lignes streamées depuis {filename} vers le topic {topic}")

        except Exception as e:
            logging.error(f"❌ Erreur de lecture du fichier {filename} : {e}")

# Définition du DAG Airflow
with DAG('multi_file_kafka_streaming',
         default_args=default_args,
         schedule=None,
         catchup=False) as dag:

    stream_task = PythonOperator(
        task_id='stream_all_files_in_folder',
        python_callable=stream_all_files_to_kafka,
        op_kwargs={'directory_path': DIRECTORY_PATH}
    )

# Exécution manuelle du script pour tests locaux (optionnel)
if __name__ == '__main__':
    stream_all_files_to_kafka(DIRECTORY_PATH)
