# configs/settings.py
"""
Configuration globale du projet
"""

# Configuration Databricks
CATALOG = "workspace"
SCHEMA = "projetbigdata"
VOLUME = "data"
PROJECT = "amazon_reviews"

# Chemins de base
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{PROJECT}"

# Architecture OBLIGATOIRE
BRONZE_MAIN = f"{BASE_PATH}/bronze/main"
BRONZE_ENRICH = f"{BASE_PATH}/bronze/enrich"
SILVER_MAIN = f"{BASE_PATH}/silver/main_clean"
SILVER_ENRICH = f"{BASE_PATH}/silver/enrich_clean"
SILVER_JOINED = f"{BASE_PATH}/silver/joined"
GOLD_MARTS = f"{BASE_PATH}/gold/marts"
GOLD_AGG = f"{BASE_PATH}/gold/aggregates"
GOLD_EXP = f"{BASE_PATH}/gold/exports"
REPORTS_QUALITY = f"{BASE_PATH}/reports/data_quality"
REPORTS_BENCH = f"{BASE_PATH}/reports/benchmarks"

# Schémas de données
from pyspark.sql.types import *

SCHEMA_AMAZON = StructType([
    StructField("Id", StringType(), False),
    StructField("ProductId", StringType(), True),
    StructField("UserId", StringType(), True),
    StructField("ProfileName", StringType(), True),
    StructField("HelpfulnessNumerator", IntegerType(), True),
    StructField("HelpfulnessDenominator", IntegerType(), True),
    StructField("Score", IntegerType(), True),
    StructField("Time", LongType(), True),
    StructField("Summary", StringType(), True),
    StructField("Text", StringType(), True)
])

SCHEMA_BEAUTY = StructType([
    StructField("UserId", StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("Rating", DoubleType(), True),
    StructField("Timestamp", LongType(), True)
])