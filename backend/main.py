from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
import os

app = FastAPI()

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Neo4j Config
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password" # User needs to set this

driver = None

@app.on_event("startup")
def startup_event():
    global driver
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print("Connected to Neo4j")
    except Exception as e:
        print(f"Failed to connect to Neo4j: {e}")

@app.on_event("shutdown")
def shutdown_event():
    if driver:
        driver.close()

@app.get("/api/risk")
def get_risk_data(hour: int):
    """
    Returns risk data for a specific hour (0-23).
    Queries the Knowledge Graph for locations with high risk during that time block.
    """
    if not driver:
        # Fallback for demo if DB is not running
        return {
            "locations": [
                {"lat": 29.6516, "lon": -82.3248, "risk": 9.5, "address": "University Ave (Mock High Risk)"},
                {"lat": 29.6190, "lon": -82.3730, "risk": 7.2, "address": "Butler Plaza (Mock High Risk)"}
            ]
        }

    day_part = "night"
    if 5 <= hour < 12: day_part = "morning"
    elif 12 <= hour < 17: day_part = "afternoon"
    elif 17 <= hour < 21: day_part = "evening"

    query = """
    MATCH (l:Location)
    OPTIONAL MATCH (l)-[r:HAS_RISK_SCORE_NIGHT]->(l) WHERE $day_part = 'night'
    OPTIONAL MATCH (l)-[r2:HAS_RISK_SCORE_DAY]->(l) WHERE $day_part IN ['morning', 'afternoon']
    WITH l, coalesce(r.score, 0) as night_score, coalesce(r2.score, 0) as day_score
    WHERE night_score > 0 OR day_score > 0
    RETURN l.lat as lat, l.lon as lon, l.address as address, 
           CASE WHEN $day_part = 'night' THEN night_score ELSE day_score END as risk
    ORDER BY risk DESC LIMIT 50
    """
    
    try:
        with driver.session() as session:
            result = session.run(query, day_part=day_part)
            locations = [{"lat": r["lat"], "lon": r["lon"], "risk": r["risk"], "address": r["address"]} for r in result]
            return {"locations": locations}
    except Exception as e:
        print(f"Query failed: {e}")
        return {"error": str(e), "locations": []}

@app.get("/")
def read_root():
    return {"status": "GainesvilleGuard API Running"}
