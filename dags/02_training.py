from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import pickle
import os


def train_correlation_model():
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # 1. Leer datos del esquema 'data'
    query = "SELECT user_id, movie_id, rating FROM data.raw_ratings"
    df = hook.get_pandas_df(query)

    # 2. Crear Matriz de Utilidad (Pivot table)
    user_movie_matrix = df.pivot_table(index='user_id', columns='movie_id', values='rating')

    # 3. Calcular Correlación de Pearson
    # Solo consideramos películas con más de 50 calificaciones para evitar ruido
    movie_corr = user_movie_matrix.corr(method='pearson', min_periods=50)

    # 4. Guardar el artefacto (Modelo)
    model_path = '/opt/airflow/scripts/movie_similarity.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(movie_corr, f)

    print(f"Modelo guardado en {model_path}")


with DAG(
        dag_id='02_training',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=['taller', 'mlops']
) as dag:
    task_train = PythonOperator(
        task_id='entrenar_matriz_correlacion',
        python_callable=train_correlation_model
    )