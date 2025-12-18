from fastapi import FastAPI, HTTPException
from app.db import fetch_one, fetch_all

app = FastAPI(title="TP MLOps - Recommendations API", version="1.0.0")

ALLOWED_MODELS = {"top_product", "top_ctr"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/recommendations/{adv}/{model}")
def recommendations(adv: str, model: str):
    model = model.lower()
    if model not in ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"Invalid model. Use: {sorted(ALLOWED_MODELS)}")

    latest = fetch_one(
        """
        SELECT MAX(date) AS max_date
        FROM recommendations
        WHERE advertiser_id = %s AND model = %s
        """,
        (adv, model),
    )
    if not latest or latest["max_date"] is None:
        raise HTTPException(status_code=404, detail="No recommendations found for advertiser/model")

    day = latest["max_date"]

    rows = fetch_all(
        """
        SELECT date, advertiser_id, model, product_id, rank, views, impressions, clicks, ctr
        FROM recommendations
        WHERE advertiser_id = %s AND model = %s AND date = %s
        ORDER BY rank ASC
        """,
        (adv, model, day),
    )

    recs = []
    for r in rows:
        item = {"rank": r["rank"], "product_id": r["product_id"]}
        if model == "top_product":
            item["views"] = r["views"]
        else:
            item["impressions"] = r["impressions"]
            item["clicks"] = r["clicks"]
            item["ctr"] = r["ctr"]
        recs.append(item)

    return {"advertiser_id": adv, "model": model, "date": str(day), "recommendations": recs, "count": len(recs)}

@app.get("/history/{adv}")
def history(adv: str):
    rows = fetch_all(
        """
        SELECT date, model, product_id, rank, views, impressions, clicks, ctr
        FROM recommendations
        WHERE advertiser_id = %s
          AND date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date DESC, model ASC, rank ASC
        """,
        (adv,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No history found for advertiser in last 7 days")

    out = {}
    for r in rows:
        d = str(r["date"])
        m = r["model"]
        out.setdefault(d, {}).setdefault(m, []).append({
            "rank": r["rank"],
            "product_id": r["product_id"],
            "views": r["views"],
            "impressions": r["impressions"],
            "clicks": r["clicks"],
            "ctr": r["ctr"],
        })

    return {"advertiser_id": adv, "history": out}

@app.get("/stats")
def stats():
    s = fetch_one(
        """
        SELECT
          COUNT(DISTINCT advertiser_id) AS advertisers_total,
          COUNT(*) AS rows_total,
          MAX(date) AS max_date,
          MIN(date) AS min_date
        FROM recommendations
        """
    )
    return {"stats": s}
