import pandas as pd
from neo4j import GraphDatabase
import os
import datetime

# --- CONFIGURATION ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"  # VALIDATE: User must update this!

class CrimeKnowledgeGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def clear_database(self):
        """Wipes the database for a fresh ETL run."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared.")

    def create_schema_constraints(self):
        """Creates indexes and constraints for performance."""
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE l.address IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Incident) REQUIRE i.case_id IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (t:TimeBlock) ON (t.hour)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Season) ON (s.name)"
        ]
        with self.driver.session() as session:
            for q in queries:
                session.run(q)
        print("Schema constraints created.")

    # --- LOADING NODES & EDGES ---
    def load_data_batch(self, df):
        """
        Batch processes the dataframe and ingests into Neo4j.
        """
        with self.driver.session() as session:
            for index, row in df.iterrows():
                # Extract attributes
                case_id = row.get("ID", f"unknown-{index}")
                incident_type = row.get("Crime Type", "Unspecified")
                address = row.get("Location", "Unknown Location")
                lat = row.get("Latitude", 0.0)
                lon = row.get("Longitude", 0.0)
                
                # Time processing
                date_str = str(row.get("Incident Date", ""))
                try:
                    dt = pd.to_datetime(date_str)
                    hour = dt.hour
                    month = dt.month
                    day_name = dt.day_name()
                except:
                    hour = 0
                    month = 1
                    day_name = "Unknown"

                # derived seasons/periods
                season = self.get_season(month)
                day_part = self.get_day_part(hour)
                
                # Cypher Query to create the subgraph for this incident
                query = """
                MERGE (l:Location {address: $address})
                ON CREATE SET l.lat = $lat, l.lon = $lon, l.type = 'street'

                MERGE (i:Incident {case_id: $case_id})
                SET i.type = $type, i.description = $type

                MERGE (tb:TimeBlock {hour: $hour})
                SET tb.part_of_day = $day_part

                MERGE (d:Day {name: $day_name})
                MERGE (s:Season {name: $season})

                MERGE (i)-[:OCCURRED_AT]->(l)
                MERGE (i)-[:OCCURRED_DURING]->(tb)
                MERGE (i)-[:ON_DAY]->(d)
                MERGE (i)-[:DURING_SEASON]->(s)
                
                // Spatial placeholder logic (to be expanded with actual geo-intersections)
                // MERGE (l)-[:LOCATED_IN]->(:Zone {name: 'ROI'}) 
                """
                
                session.run(query, 
                            address=address, lat=lat, lon=lon,
                            case_id=case_id, type=incident_type,
                            hour=hour, day_part=day_part,
                            day_name=day_name, season=season)
                
            print(f"Processed {len(df)} rows.")

    def calculate_risk_scores(self):
        """
        Post-processing: Analyze incidents to create RISK edges.
        Example: If a location has many crimes at night, create [:HAS_RISK_SCORE_NIGHT]
        """
        query = """
        MATCH (l:Location)<-[:OCCURRED_AT]-(i:Incident)-[:OCCURRED_DURING]->(tb:TimeBlock)
        WITH l, tb.part_of_day as part_of_day, count(i) as incident_count
        WHERE incident_count > 0
        WITH l, part_of_day, incident_count
        
        // Dynamically create risk edges based on time of day
        CALL apoc.do.when(
            part_of_day = 'night',
            'MERGE (l)-[r:HAS_RISK_SCORE_NIGHT]->(l) SET r.score = incident_count',
            'RETURN l'
        ) YIELD value
        RETURN count(l)
        """
        # Note: APOC might not be available. Using standard cypher simplified:
        
        print("Calculating risk scores...")
        with self.driver.session() as session:
            # Night Risk
            session.run("""
                MATCH (l:Location)<-[:OCCURRED_AT]-(i:Incident)-[:OCCURRED_DURING]->(tb:TimeBlock)
                WHERE tb.part_of_day = 'night'
                WITH l, count(i) as risk_score
                MERGE (l)-[r:HAS_RISK_SCORE_NIGHT]->(l)
                SET r.score = risk_score
            """)
            
            # Day Risk
            session.run("""
                MATCH (l:Location)<-[:OCCURRED_AT]-(i:Incident)-[:OCCURRED_DURING]->(tb:TimeBlock)
                WHERE tb.part_of_day IN ['morning', 'afternoon']
                WITH l, count(i) as risk_score
                MERGE (l)-[r:HAS_RISK_SCORE_DAY]->(l)
                SET r.score = risk_score
            """)
        print("Risk scores updated.")

    @staticmethod
    def get_season(month):
        if month in [12, 1, 2]: return "Winter"
        elif month in [3, 4, 5]: return "Spring"
        elif month in [6, 7, 8]: return "Summer"
        else: return "Fall"

    @staticmethod
    def get_day_part(hour):
        if 5 <= hour < 12: return "morning"
        elif 12 <= hour < 17: return "afternoon"
        elif 17 <= hour < 21: return "evening"
        else: return "night"

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Check for data file
    data_path = "../data/crime_data.csv"
    if not os.path.exists(data_path):
        print(f"ERROR: Data file not found at {data_path}")
        print("Please place your CSV file in the 'data' folder.")
        # Create a dummy CSV for demonstration if it doesn't exist
        with open(data_path, 'w') as f:
            f.write("ID,Crime Type,Location,Latitude,Longitude,Incident Date\n")
            f.write("1,Theft,University Ave & 13th St,29.6516,-82.3248,2023-10-31 23:00:00\n")
            f.write("2,Assault,34th St & Archer Rd,29.6190,-82.3730,2023-11-01 14:30:00\n")
        print("Created dummy data for testing.")

    # Load Data
    try:
        df = pd.read_csv(data_path)
        
        # Initialize Graph
        # NOTE: Using default locals for now. 
        kg = CrimeKnowledgeGraph(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        
        kg.clear_database()
        kg.create_schema_constraints()
        kg.load_data_batch(df)
        kg.calculate_risk_scores()
        
        kg.close()
        print("ETL Pipeline Complete!")
        
    except Exception as e:
        print(f"ETL Failed: {e}")
