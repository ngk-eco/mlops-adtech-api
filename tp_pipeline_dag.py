from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
from psycopg2.extras import execute_values

import boto3

# --------------------------------------
# Configuración básica de rutas
# --------------------------------------
RAW_DATA_DIR = "/home/ec2-user/airflow/data_raw"
PROCESSED_DATA_DIR = "/home/ec2-user/airflow/data_processed"

ADVERTISERS_PATH = f"{RAW_DATA_DIR}/advertiser_ids.csv"
PRODUCT_VIEWS_PATH = f"{RAW_DATA_DIR}/product_views.csv"
ADS_VIEWS_PATH = f"{RAW_DATA_DIR}/ads_views.csv"

PRODUCT_VIEWS_FILTERED_PATH = f"{PROCESSED_DATA_DIR}/product_views_filtered.csv"
ADS_VIEWS_FILTERED_PATH = f"{PROCESSED_DATA_DIR}/ads_views_filtered.csv"

TOP_PRODUCT_PATH = f"{PROCESSED_DATA_DIR}/top_product.csv"
TOP_CTR_PATH = f"{PROCESSED_DATA_DIR}/top_ctr.csv"
RECOMMENDATIONS_PATH = f"{PROCESSED_DATA_DIR}/recommendations.csv"

# --------------------------------------
# Configuración de la base de datos RDS
# --------------------------------------
DB_HOST = "mlops-tp-db.cf4i6e6cwv74.us-east-1.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "grupo_6_2025"
DB_PASSWORD = "NubeMLOPS!"

# Bucket S3
S3_BUCKET = "grupo-6-2025-s3"


# --------------------------------------
# Funciones de cada tarea
# --------------------------------------

def filtrar_datos():
    """
    Descarga los CSV crudos desde S3 a disco local,
    filtra por advertisers activos y guarda CSV filtrados
    tanto localmente como en S3 (resultados intermedios).
    """
    s3 = boto3.client("s3")

    # 1) Descargar archivos de entrada desde S3 a las rutas locales
    s3.download_file(S3_BUCKET, "advertiser_ids.csv", ADVERTISERS_PATH)
    s3.download_file(S3_BUCKET, "product_views.csv", PRODUCT_VIEWS_PATH)
    s3.download_file(S3_BUCKET, "ads_views.csv", ADS_VIEWS_PATH)

    # 2) Leer y filtrar
    advertisers = pd.read_csv(ADVERTISERS_PATH)
    product_views = pd.read_csv(PRODUCT_VIEWS_PATH)
    ads_views = pd.read_csv(ADS_VIEWS_PATH)

    active_ids = advertisers["advertiser_id"].unique()

    product_views_filtered = product_views[
        product_views["advertiser_id"].isin(active_ids)
    ].copy()

    ads_views_filtered = ads_views[
        ads_views["advertiser_id"].isin(active_ids)
    ].copy()

    # Guardar local
    product_views_filtered.to_csv(PRODUCT_VIEWS_FILTERED_PATH, index=False)
    ads_views_filtered.to_csv(ADS_VIEWS_FILTERED_PATH, index=False)

    # 3) Subir resultados filtrados a S3 como salidas intermedias
    s3.upload_file(
        PRODUCT_VIEWS_FILTERED_PATH,
        S3_BUCKET,
        "processed/product_views_filtered.csv",
    )
    s3.upload_file(
        ADS_VIEWS_FILTERED_PATH,
        S3_BUCKET,
        "processed/ads_views_filtered.csv",
    )


def calcular_top_product():
    """
    Calcula TopProduct a partir del log filtrado de vistas de producto.
    Guarda el resultado en top_product.csv (local) y también en S3.
    """
    product_views_filtered = pd.read_csv(PRODUCT_VIEWS_FILTERED_PATH)

    views = (
        product_views_filtered
        .groupby(["advertiser_id", "product_id"])
        .size()
        .reset_index(name="views")
    )

    views = views.sort_values(
        by=["advertiser_id", "views"],
        ascending=[True, False]
    )

    top_product = (
        views
        .groupby("advertiser_id")
        .head(20)
        .copy()
    )

    top_product["model"] = "top_product"
    top_product["rank"] = (
        top_product
        .groupby("advertiser_id")
        .cumcount() + 1
    )

    # Guardar local
    top_product.to_csv(TOP_PRODUCT_PATH, index=False)

    # Subir a S3 como resultado intermedio
    s3 = boto3.client("s3")
    s3.upload_file(
        TOP_PRODUCT_PATH,
        S3_BUCKET,
        "processed/top_product.csv",
    )


def calcular_top_ctr():
    """
    Calcula TopCTR a partir del log filtrado de vistas de ads.
    Guarda el resultado en top_ctr.csv (local) y también en S3.
    """
    ads_views_filtered = pd.read_csv(ADS_VIEWS_FILTERED_PATH)

    df = ads_views_filtered.copy()
    df["impression"] = (df["type"] == "impression").astype(int)
    df["click"] = (df["type"] == "click").astype(int)

    agg = (
        df.groupby(["advertiser_id", "product_id"])
        .agg(
            impressions=("impression", "sum"),
            clicks=("click", "sum"),
        )
        .reset_index()
    )

    # Nos quedamos con productos que tengan al menos 1 impresión
    agg = agg[agg["impressions"] > 0].copy()

    agg["ctr"] = agg["clicks"] / agg["impressions"]

    agg = agg.sort_values(
        by=["advertiser_id", "ctr"],
        ascending=[True, False]
    )

    top_ctr = (
        agg
        .groupby("advertiser_id")
        .head(20)
        .copy()
    )

    top_ctr["model"] = "top_ctr"
    top_ctr["rank"] = (
        top_ctr
        .groupby("advertiser_id")
        .cumcount() + 1
    )

    # Guardar local
    top_ctr.to_csv(TOP_CTR_PATH, index=False)

    # Subir a S3 como resultado intermedio
    s3 = boto3.client("s3")
    s3.upload_file(
        TOP_CTR_PATH,
        S3_BUCKET,
        "processed/top_ctr.csv",
    )


def db_writing(**context):
    """
    Lee los CSV de top_product y top_ctr, los combina en un único
    DataFrame y lo inserta en la tabla 'recommendations' de RDS.
    """
    # Fecha de ejecución del DAG (YYYY-MM-DD) que Airflow pasa en el contexto
    execution_date = context["ds"]  # string, ej: "2025-12-03"

    # 1) Leer los CSV generados por las tasks anteriores (locales)
    top_product = pd.read_csv(TOP_PRODUCT_PATH)
    top_ctr = pd.read_csv(TOP_CTR_PATH)

    # Asegurar columnas para que sean compatibles
    # TopProduct no tiene columnas de ads
    top_product["impressions"] = None
    top_product["clicks"] = None
    top_product["ctr"] = None

    # TopCTR no tiene 'views'
    if "views" not in top_ctr.columns:
        top_ctr["views"] = None

    # 2) Combinar ambos modelos en un solo DataFrame
    recommendations = pd.concat(
        [top_product, top_ctr],
        ignore_index=True,
        sort=False,
    )

    # 3) Agregar columna 'date' con la fecha de ejecución
    recommendations["date"] = execution_date

    # 4) Reordenar columnas para que coincidan con la tabla SQL
    columns = [
        "date",
        "advertiser_id",
        "model",
        "product_id",
        "rank",
        "views",
        "impressions",
        "clicks",
        "ctr",
    ]
    recommendations = recommendations[columns]

    # 5) Convertir a lista de tuplas para psycopg2
    records = [tuple(row) for row in recommendations.to_numpy()]

    insert_sql = """
        INSERT INTO recommendations (
            date,
            advertiser_id,
            model,
            product_id,
            rank,
            views,
            impressions,
            clicks,
            ctr
        ) VALUES %s
    """

    # 6) Conectar a la base en RDS y hacer el insert masivo
    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            # Opcional: borrar datos previos de ese día para evitar duplicados
            cur.execute(
                "DELETE FROM recommendations WHERE date = %s",
                (execution_date,),
            )

            # Insert masivo
            execute_values(cur, insert_sql, records)

        conn.commit()


# --------------------------------------
# Definición del DAG
# --------------------------------------

default_args = {
    "owner": "mati",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tp_recommendations_pipeline",
    default_args=default_args,
    description="Pipeline diario para generar recomendaciones TopProduct y TopCTR",
    schedule_interval="0 0 * * *",  # todos los días a la medianoche
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["mlops", "tp", "adtech"],
) as dag:

    filtrar_datos_task = PythonOperator(
        task_id="FiltrarDatos",
        python_callable=filtrar_datos,
    )

    top_product_task = PythonOperator(
        task_id="TopProduct",
        python_callable=calcular_top_product,
    )

    top_ctr_task = PythonOperator(
        task_id="TopCTR",
        python_callable=calcular_top_ctr,
    )

    db_writing_task = PythonOperator(
        task_id="DBWriting",
        python_callable=db_writing,
        op_kwargs={},  # usamos context["ds"]
    )

    # Orden de ejecución
    filtrar_datos_task >> top_product_task >> top_ctr_task >> db_writing_task
