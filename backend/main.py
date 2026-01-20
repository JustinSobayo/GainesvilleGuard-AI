from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from backend.neo4j_ops import get_crime_explanation

app = FastAPI(title="GainesvilleGuard API")

@app.get("/")
def health_check():
    return {"status": "ok", "service": "GainesvilleGuard-AI"}

@app.get("/crimes/history")
def get_historical_crimes(date: str):
    """
    TODO: Query Postgres for crimes on a specific date.
    Returns: List of crimes with lat/lon.
    """
    pass

@app.get("/crimes/predict")
def get_crime_predictions():
    """
    TODO: Query Postgres/Neo4j for high-risk grids.
    Returns: GeoJSON of red circles.
    """
    pass

@app.get("/predict/explain")
def explain_prediction(grid_id: str):
    """
    TODO: Use LangChain to explain a risk score.
    Returns: Text explanation.
    """
    pass
