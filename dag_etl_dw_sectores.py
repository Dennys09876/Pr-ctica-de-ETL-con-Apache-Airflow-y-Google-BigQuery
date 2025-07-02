
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Funciones simuladas para el ETL
def extraer():
    print("Extrayendo datos desde fuente CSV/API...")

def transformar():
    print("Transformando datos (limpieza, agregación)...")

def cargar():
    print("Cargando datos a BigQuery Data Warehouse...")

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_dw_sectores_economicos',
    default_args=default_args,
    description='DAG ETL para sectores económicos del SRI',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 29),
    catchup=False,
    tags=['ETL', 'SRI', 'BigQuery']
) as dag:

    t1 = PythonOperator(
        task_id='extraer_datos',
        python_callable=extraer
    )

    t2 = PythonOperator(
        task_id='transformar_datos',
        python_callable=transformar
    )

    t3 = PythonOperator(
        task_id='cargar_datos',
        python_callable=cargar
    )

    t1 >> t2 >> t3
