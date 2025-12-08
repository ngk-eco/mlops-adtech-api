import os
from datetime import date, timedelta
from typing import Dict, Any
import math

import psycopg2
from fastapi import FastAPI, HTTPException

# ---------------------------------------------------
# Configuración de la base de datos RDS
# (usa env vars si existen, sino los defaults)
# ---------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "mlops-tp-db.cf4i6e6cwv74.us-east-1.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "grupo_6_2025")
DB_PASSWORD = os.getenv("DB_PASSWORD", "NubeMLOPS!")


def get_connection():
    """
    Crea una conexión nueva a la base de datos.
    Para este TP el tráfico es bajo, así que abrir y cerrar
    por request está bien.
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def safe_float(value):
    """
    Convierte a float y limpia NaN / +/-inf para que sean JSON-valid.
    Devuelve None si no es un número representable.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None   # o 0.0 si preferís
    return f


# ---------------------------------------------------
# Instancia de FastAPI
# ---------------------------------------------------
app = FastAPI(
    title="TP Final MLOps - Recomendaciones AdTech",
    description="API para servir recomendaciones desde la tabla 'recommendations' en RDS.",
    version="1.0.0",
)


# ---------------------------------------------------
# Endpoints básicos: / y /health
# ---------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "mlops-tp-api"}


@app.get("/health")
def health():
    # opcional: probar conexión a DB
    try:
        conn = get_connection()
        conn.close()
        db_status = "ok"
    except Exception:
        db_status = "error"

    return {
        "status": "ok",
        "db_status": db_status,
    }


# ---------------------------------------------------
# Endpoint 1: /recommendations/{adv}/{model}
# ---------------------------------------------------
@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: str, model: str) -> Dict[str, Any]:
    """
    Devuelve las recomendaciones del día actual para
    un advertiser (adv) y un modelo dado (top_product o top_ctr).
    """

    model_normalized = model.lower()
    if model_normalized not in ("top_product", "top_ctr"):
        raise HTTPException(
            status_code=400,
            detail="Modelo inválido. Use 'top_product' o 'top_ctr'.",
        )

    today = date.today()  # o se podría parametrizar

    query = """
        SELECT
            date,
            advertiser_id,
            model,
            product_id,
            rank,
            views,
            impressions,
            clicks,
            ctr
        FROM recommendations
        WHERE
            date = %s
            AND advertiser_id = %s
            AND model = %s
        ORDER BY rank ASC;
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (today, adv, model_normalized))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No hay recomendaciones para advertiser={adv}, modelo={model_normalized} en la fecha {today}.",
        )

    recs = []
    for r in rows:
        recs.append(
            {
                "date": r[0].isoformat(),
                "advertiser_id": r[1],
                "model": r[2],
                "product_id": r[3],
                "rank": r[4],
                "views": r[5],
                "impressions": r[6],
                "clicks": r[7],
                "ctr": safe_float(r[8]),
            }
        )

    return {
        "date": today.isoformat(),
        "advertiser_id": adv,
        "model": model_normalized,
        "num_recommendations": len(recs),
        "recommendations": recs,
    }


# ---------------------------------------------------
# Endpoint 2: /history/{adv}
# ---------------------------------------------------
@app.get("/history/{adv}")
def get_history(adv: str) -> Dict[str, Any]:
    """
    Devuelve todas las recomendaciones para un advertiser
    en los últimos 7 días (incluyendo hoy).
    """

    today = date.today()
    seven_days_ago = today - timedelta(days=7)

    query = """
        SELECT
            date,
            advertiser_id,
            model,
            product_id,
            rank,
            views,
            impressions,
            clicks,
            ctr
        FROM recommendations
        WHERE
            advertiser_id = %s
            AND date >= %s
            AND date <= %s
        ORDER BY date DESC, model, rank ASC;
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (adv, seven_days_ago, today))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No hay historial de recomendaciones para advertiser={adv} en los últimos 7 días.",
        )

    recs = []
    for r in rows:
        recs.append(
            {
                "date": r[0].isoformat(),
                "advertiser_id": r[1],
                "model": r[2],
                "product_id": r[3],
                "rank": r[4],
                "views": r[5],
                "impressions": r[6],
                "clicks": r[7],
                "ctr": safe_float(r[8]),
            }
        )

    return {
        "advertiser_id": adv,
        "from_date": seven_days_ago.isoformat(),
        "to_date": today.isoformat(),
        "num_records": len(recs),
        "history": recs,
    }


# ---------------------------------------------------
# Endpoint 3: /stats
# ---------------------------------------------------
@app.get("/stats")
def get_stats() -> Dict[str, Any]:
    """
    Devuelve estadísticas simples sobre las recomendaciones.
    """

    today = date.today()

    conn = get_connection()
    try:
        cur = conn.cursor()

        # 1) Cantidad de advertisers distintos hoy
        cur.execute(
            """
            SELECT COUNT(DISTINCT advertiser_id)
            FROM recommendations
            WHERE date = %s;
            """,
            (today,),
        )
        num_advertisers = cur.fetchone()[0] or 0

        # 2) Cantidad total de recomendaciones hoy
        cur.execute(
            """
            SELECT COUNT(*)
            FROM recommendations
            WHERE date = %s;
            """,
            (today,),
        )
        total_recs = cur.fetchone()[0] or 0

        # 3) Productos por advertiser y modelo
        cur.execute(
            """
            SELECT advertiser_id, model, product_id
            FROM recommendations
            WHERE date = %s;
            """,
            (today,),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    # adv -> {"top_product": set(...), "top_ctr": set(...)}
    adv_models: Dict[str, Dict[str, set]] = {}
    for adv_id, model, product_id in rows:
        if adv_id not in adv_models:
            adv_models[adv_id] = {"top_product": set(), "top_ctr": set()}
        if model in ("top_product", "top_ctr"):
            adv_models[adv_id][model].add(product_id)

    overlaps = []
    for adv_id, models in adv_models.items():
        tp = models["top_product"]
        tc = models["top_ctr"]
        if tp and tc:
            inter = tp.intersection(tc)
            union = tp.union(tc)
            jaccard = len(inter) / len(union) if union else 0.0
            overlaps.append(
                {
                    "advertiser_id": adv_id,
                    "intersection_size": len(inter),
                    "union_size": len(union),
                    "jaccard": jaccard,
                }
            )

    if overlaps:
        avg_jaccard = sum(o["jaccard"] for o in overlaps) / len(overlaps)
    else:
        avg_jaccard = 0.0

    overlaps_sorted = sorted(overlaps, key=lambda x: x["jaccard"], reverse=True)
    top_5_mas_similares = overlaps_sorted[:5]
    top_5_menos_similares = list(reversed(overlaps_sorted[-5:]))

    return {
        "date": today.isoformat(),
        "num_advertisers_today": num_advertisers,
        "total_recommendations_today": total_recs,
        "avg_jaccard_top_product_vs_top_ctr": avg_jaccard,
        "top_5_advertisers_mas_similares": top_5_mas_similares,
        "top_5_advertisers_menos_similares": top_5_menos_similares,
    }
