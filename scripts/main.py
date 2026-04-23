from fastapi import FastAPI, HTTPException
import pandas as pd
import pickle
import os
from sqlalchemy import create_engine

app = FastAPI(title="MovieLens Workshop API")

# Conexión a la DB (Esquema 'data')
DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"
engine = create_engine(DB_URL)

MODEL_PATH = "/app/movie_similarity.pkl"


@app.get("/")
def health_check():
    model_status = os.path.exists(MODEL_PATH)
    return {
        "status": "online",
        "model_loaded": model_status,
        "message": "Si model_loaded es false, ejecuta el DAG 02 en Airflow"
    }


@app.get("/recommend/{movie_id}")
def recommend(movie_id: int):
    if not os.path.exists(MODEL_PATH):
        raise HTTPException(status_code=503, detail="Modelo no disponible. Entrena el modelo en Airflow primero.")

    with open(MODEL_PATH, 'rb') as f:
        movie_corr = pickle.load(f)

    if movie_id not in movie_corr.columns:
        raise HTTPException(status_code=404, detail="Película no encontrada en la matriz.")

    # Lógica de recomendación
    similar_scores = movie_corr[movie_id].dropna().sort_values(ascending=False)[1:6]
    ids = similar_scores.index.tolist()

    # Obtener nombres de la tabla que creó el DAG 01
    query = f"SELECT movie_id, title FROM data.raw_movies WHERE movie_id IN ({','.join(map(str, ids))})"
    names_df = pd.read_sql(query, engine)

    return names_df.to_dict(orient="records")