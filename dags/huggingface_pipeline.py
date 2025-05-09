from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datasets import load_dataset
import pandas as pd
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}




def extract_data():
    dataset = load_dataset("imdb")
    print(dataset)

def transform_data(**kwargs):
    df = pd.read_csv("/opt/airflow/data/raw_imdb.csv")
    # On garde uniquement les colonnes utiles et supprime les nulls
    df_clean = df[['text', 'label']].dropna()
    df_clean.to_csv("/opt/airflow/output/cleaned_imdb.csv", index=False)

def load_to_mongodb(**kwargs):
    df = pd.read_csv("/opt/airflow/output/cleaned_imdb.csv")
    
    # Connexion à MongoDB (conteneur "mongodb")
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["huggingface_db"]
    collection = db["imdb_reviews"]

    # Insertion dans la collection
    collection.insert_many(df.to_dict(orient="records"))
    client.close()

with DAG(
    dag_id='huggingface_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
