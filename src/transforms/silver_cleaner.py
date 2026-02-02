# src/transforms/silver_cleaner.py
"""
Module de nettoyage et transformation Silver
"""

from pyspark.sql.functions import col, from_unixtime, expr, lit
from pyspark.sql.dataframe import DataFrame

def clean_amazon_reviews(df_raw: DataFrame, run_id: str, run_date: str) -> DataFrame:
    """
    Nettoyage du dataset Amazon Reviews pour la couche Silver
    
    Args:
        df_raw: DataFrame brut depuis Bronze
        run_id: Identifiant d'exécution
        run_date: Date d'exécution
        
    Returns:
        DataFrame nettoyé
    """
    print("🔧 Transformation Silver - Amazon Reviews...")
    
    df_cleaned = (df_raw
        .withColumn("Score", expr("try_cast(Score as int)"))
        .withColumn("HelpfulnessNumerator", expr("try_cast(HelpfulnessNumerator as int)"))
        .withColumn("HelpfulnessDenominator", expr("try_cast(HelpfulnessDenominator as int)"))
        .withColumn("Time", expr("try_cast(Time as long)"))
        
        # Filtres de qualité
        .filter(col("Score").isNotNull())
        .filter(col("Score").between(1, 5))
        .filter(col("Id").isNotNull())
        .filter(col("ProductId").isNotNull())
        .filter(col("UserId").isNotNull())
        .filter(col("ProfileName").isNotNull())
        
        # Dédoublonnage
        .dropDuplicates(["Id"])
        
        # Feature de date
        .withColumn("ReviewDate", from_unixtime(col("Time")).cast("timestamp"))
        
        # Métadonnées d'exécution
        .withColumn("run_id", lit(run_id))
        .withColumn("run_date", lit(run_date))
    )
    
    print(f"✅ Lignes après nettoyage: {df_cleaned.count():,}")
    print(f"📅 Colonnes ajoutées: ReviewDate, run_id, run_date")
    
    return df_cleaned

def clean_beauty_ratings(df_raw: DataFrame, run_id: str, run_date: str) -> DataFrame:
    """
    Nettoyage du dataset Beauty Ratings pour la couche Silver
    
    Args:
        df_raw: DataFrame brut depuis Bronze
        run_id: Identifiant d'exécution
        run_date: Date d'exécution
        
    Returns:
        DataFrame nettoyé
    """
    print("🔧 Transformation Silver - Beauty Ratings...")
    
    df_cleaned = (df_raw
        .filter(col("Rating").between(1, 5))
        .filter(col("UserId").isNotNull())
        .filter(col("ProductId").isNotNull())
        .dropDuplicates(["UserId", "ProductId"])
        .withColumn("run_id", lit(run_id))
        .withColumn("run_date", lit(run_date))
    )
    
    print(f"✅ Lignes après nettoyage: {df_cleaned.count():,}")
    
    return df_cleaned

def write_silver_layer(df: DataFrame, output_path: str):
    """
    Écriture des données Silver en Delta Lake avec partitionnement
    
    Args:
        df: DataFrame à écrire
        output_path: Chemin de sortie
    """
    print(f"💾 Écriture Silver: {output_path}")
    
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("run_date") \
        .save(output_path)
    
    print(f"✅ Écriture terminée: {output_path}")