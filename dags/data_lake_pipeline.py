from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# 1. TRANSFORMATION
def transform_csv():
    input_path = '/opt/airflow/data/example.csv'
    output_path = '/opt/airflow/output/filtered.csv'
    
    df = pd.read_csv(input_path)
    filtered_df = df[df['age'] > 28]
    filtered_df.to_csv(output_path, index=False)

# 2. CHARGEMENT DANS POSTGRES
def load_to_postgres(**kwargs):
    df = pd.read_csv('/opt/airflow/output/filtered.csv')

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cur = conn.cursor()

    # S'assurer que la table existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS people (
            name VARCHAR(50),
            age INTEGER
        );
    """)

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO people (name, age) VALUES (%s, %s)",
            (row['name'], int(row['age']))
        )

    conn.commit()
    cur.close()
    conn.close()

# 3. CHARGEMENT DANS MONGODB
def load_to_mongo(**kwargs):
    df = pd.read_csv('/opt/airflow/output/filtered.csv')

    client = MongoClient("mongodb://mongodb:27017/")
    db = client["airflow"]
    collection = db["people"]

    records = df.to_dict(orient='records')
    collection.insert_many(records)

# DAG DEFINITION
with DAG(
    dag_id='data_lake_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task_transform = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv
    )

    task_load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    task_load_mongo = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
        provide_context=True
    )

    # CHAÎNE DES TÂCHES
    task_transform >> task_load_postgres >> task_load_mongo
