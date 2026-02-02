# src/ingestion/bronze_loader.py
"""
Module d'ingestion des données vers Bronze
"""

from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, current_date
from datetime import datetime

def load_with_schema(path: str, schema: StructType, dataset_name: str = "dataset"):
    """
    Chargement d'un dataset avec schéma explicite
    
    Args:
        path: Chemin des données
        schema: Schéma explicite
        dataset_name: Nom du dataset pour logging
        
    Returns:
        DataFrame chargé
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    print(f"📥 Chargement {dataset_name}: {path}")
    
    df = spark.read.schema(schema).parquet(path)
    
    print(f"✅ {dataset_name} chargé: {df.count():,} lignes, {len(df.columns)} colonnes")
    
    return df

def write_bronze_layer(df, output_path: str, run_id: str):
    """
    Écriture des données Bronze avec idempotence
    
    Args:
        df: DataFrame à écrire
        output_path: Chemin de sortie
        run_id: Identifiant d'exécution
    """
    # Ajouter run_date pour idempotence
    df_with_metadata = df \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("run_date", lit(current_date()))
    
    print(f"💾 Écriture Bronze: {output_path}")
    
    df_with_metadata.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("run_date") \
        .save(output_path)
    
    print(f"✅ Bronze écrit: {output_path}")