"""
Crime Data Processor (Spark Structured Streaming)

Consumes crime data from Kafka, transforms it, and writes to:
- PostgreSQL/PostGIS for geospatial analysis
- Neo4j for knowledge graph / Graph RAG

Data Flow: Kafka (raw_crime_data topic) --> Spark --> Postgres + Neo4j
"""

import os

from dotenv import load_dotenv


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'raw_crime_data'

# PostgreSQL connection
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'gatorguard')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

# Neo4j connection
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')


def get_spark_session():
    """    
    Steps:
    1. Use SparkSession.builder
    2. Set appName to "CrimeDataProcessor"
    3. Add packages for Kafka and PostgreSQL JDBC:
       - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
       - org.postgresql:postgresql:42.6.0
    """
    try:
        return SparkSession.builder \
            .appName("CrimeDataProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
            .getOrCreate()
    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        raise e

def get_crime_schema():

    return StructType([
        StructField("id", StringType(), True),
        StructField("Incident_Type", StringType(), True),
        StructField("report_date", StringType(), True),
        StructField("offense_date", StringType(), True),
        StructField("offense_hour_of_day", StringType(), True),
        StructField("offense_day_of_week", StringType(), True),
        StructField("address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
    ])


def read_from_kafka(spark):
    """    
    The returned DataFrame will have columns: key, value, topic, partition, offset, timestamp
    The 'value' column contains your JSON as bytes.
    """
    try:
        return spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", TOPIC_NAME) \
            .option("startingOffsets", "earliest") \
            .load()
    except Exception as e:
        print(f"Error reading from Kafka: {e}")
        raise e


def transform_crime_data(df, schema):
    """    
    Return the transformed DataFrame.
    """
    try:
        return df.withColumn("value", col("value").cast("string")) \
            .withColumn("json", from_json(col("value"), schema)) \
            .select("json.*") \
            .withColumn("geometry", concat(lit("POINT("), col("longitude"), lit(" "), col("latitude"), lit(")")))
    except Exception as e:
        print(f"Error transforming crime data: {e}")
        raise e


def write_to_postgres(df, table_name: str):
    try:
        def write_batch(batch_df, batch_id):
            batch_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        
        return df.writeStream.foreachBatch(write_batch)
    except Exception as e:
        print(f"Error executing Postgres write strategy: {e}")
        raise e
def write_to_neo4j(df):
    """
    Write crime data to Neo4j knowledge graph using foreachBatch.
    """
    from neo4j import GraphDatabase
    
    def get_season(month):
        if month in [12, 1, 2]: return "Winter"
        elif month in [3, 4, 5]: return "Spring"
        elif month in [6, 7, 8]: return "Summer"
        else: return "Fall"
    
    def get_day_part(hour):
        if 5 <= hour < 12: return "morning"
        elif 12 <= hour < 17: return "afternoon"
        elif 17 <= hour < 21: return "evening"
        else: return "night"
    
    def write_batch_to_neo4j(batch_df, batch_id):
        """Process each micro-batch and write to Neo4j."""
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        try:
            with driver.session() as session:
                # Convert Spark DataFrame to list of dicts for processing
                rows = batch_df.collect()
                
                for row in rows:
                    # Extract attributes from row
                    case_id = row['id'] if row['id'] else f"unknown-{batch_id}"
                    incident_type = row['Incident_Type'] if row['Incident_Type'] else "Unspecified"
                    address = row['address'] if row['address'] else "Unknown Location"
                    lat = row['latitude'] if row['latitude'] else 0.0
                    lon = row['longitude'] if row['longitude'] else 0.0
                    
                    # Time processing
                    hour = int(row['offense_hour_of_day']) if row['offense_hour_of_day'] else 0
                    day_name = row['offense_day_of_week'] if row['offense_day_of_week'] else "Unknown"
                    
                    # Parse month from offense_date for season calculation
                    try:
                        from datetime import datetime
                        offense_date = row['offense_date']
                        if offense_date:
                            dt = datetime.fromisoformat(str(offense_date).replace('Z', '+00:00'))
                            month = dt.month
                        else:
                            month = 1
                    except:
                        month = 1
                    
                    season = get_season(month)
                    day_part = get_day_part(hour)
                    
                    # Cypher query
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
                    """
                    
                    session.run(query,
                                address=address, lat=lat, lon=lon,
                                case_id=case_id, type=incident_type,
                                hour=hour, day_part=day_part,
                                day_name=day_name, season=season)
                
                print(f"Batch {batch_id}: Wrote {len(rows)} records to Neo4j")
        except Exception as e:
            print(f"Error writing batch {batch_id} to Neo4j: {e}")
            raise e
        finally:
            driver.close()
    
    return df.writeStream.foreachBatch(write_batch_to_neo4j)


def process_stream():
    print("Crime Processor starting...")
    print(f"Reading from Kafka: {KAFKA_BROKER}/{TOPIC_NAME}")
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    schema = get_crime_schema()
    
    raw_df = read_from_kafka(spark)
    
    transformed_df = transform_crime_data(raw_df, schema)
    
    # 3. Write to Postgres (Stream 1)
    print(f"Starting Postgres stream to table: historical_crimes")
    postgres_query = write_to_postgres(transformed_df, "historical_crimes").start()
    
    # 4. Write to Neo4j (Stream 2)
    print(f"Starting Neo4j stream...")
    neo4j_query = write_to_neo4j(transformed_df).start()
    
    # 5. Wait for termination
    print("Streams started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    process_stream()
