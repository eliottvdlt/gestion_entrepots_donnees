from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Ajouter le chemin du dossier contenant `api_to_mongo.py` au PATH
sys.path.append('/opt/airflow/dags')  # Remplace par le chemin exact si nécessaire

# Importer la fonction principale du script
from api_to_mongo import main as execute_api_to_mongo

# Assurer que les variables d'environnement sont correctement chargées
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/airflow/dags/.env")  # Remplace par le chemin correct de ton fichier .env

# Arguments par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
with DAG(
    'api_to_mongo_dag',
    default_args=default_args,
    description='Chargement des données API dans MongoDB',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Tâche principale
    run_task = PythonOperator(
        task_id='api_to_mongo_task',
        python_callable=execute_api_to_mongo,  # Appelle la fonction principale du script
    )
