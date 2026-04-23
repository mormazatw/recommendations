from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import requests
import zipfile
import io


def download_and_load():
    # 1. Descarga del dataset pequeño
    url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    response = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(response.content))

    # 2. Cargar los archivos clave en DataFrames
    # 'ratings.csv' contiene user_id, movie_id, rating
    with z.open('ml-latest-small/ratings.csv') as file:
        df_ratings = pd.read_csv(file)

    # 'movies.csv' contiene movie_id, title, genres
    with z.open('ml-latest-small/movies.csv') as file:
        df_movies = pd.read_csv(file)

    # 3. Conexión a Postgres y carga
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    # Agrega schema='data' en ambos
    df_ratings.to_sql('raw_ratings', engine, schema='data', if_exists='replace', index=False)
    df_movies.to_sql('raw_movies', engine, schema='data', if_exists='replace', index=False)

    print(f"Éxito: {len(df_ratings)} ratings y {len(df_movies)} películas cargadas.")


with DAG(
        dag_id='01_ingesta_movielens',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,  # Si le ponemos None, la ejecución es manual
        catchup=False,
        tags=['taller', 'mlops']
) as dag:
    task_ingesta = PythonOperator(
        task_id='descargar_y_cargar_datos',
        python_callable=download_and_load
    )