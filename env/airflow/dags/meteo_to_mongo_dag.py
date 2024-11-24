from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from meteo_to_mongo import fetch_and_store_meteo_data  # Assurez-vous que le chemin est correct

# Définir les arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
with DAG(
    dag_id="meteo_to_mongo_dag",
    default_args=default_args,
    description="DAG pour importer les données météo dans MongoDB",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    fetch_meteo_data = PythonOperator(
        task_id="fetch_meteo_data",
        python_callable=fetch_and_store_meteo_data,
    )

    fetch_meteo_data
