# src/transforms/gold_builder.py
"""
Module de création de la couche Gold
"""

from pyspark.sql.functions import year, month, avg, count, col

def create_gold_mart(df_silver):
    """
    Crée le mart principal (table analytique)
    
    Args:
        df_silver: DataFrame Silver enrichi
        
    Returns:
        DataFrame mart
    """
    print("🏆 Création Gold Mart...")
    
    df_mart = df_silver.select(
        "Id", "UserId", "ProductId",
        "Score", "Rating", "score_diff", "has_beauty_rating",
        "ReviewDate", "HelpfulnessNumerator", "HelpfulnessDenominator",
        "run_id", "run_date"
    )
    
    print(f"✅ Mart créé: {df_mart.count():,} lignes")
    
    return df_mart

def create_monthly_aggregation(df_mart):
    """
    Crée l'agrégation temporelle mensuelle
    
    Args:
        df_mart: DataFrame mart
        
    Returns:
        DataFrame d'agrégation
    """
    print("📅 Création agrégation mensuelle...")
    
    df_monthly = (df_mart
        .withColumn("year", year("ReviewDate"))
        .withColumn("month", month("ReviewDate"))
        .filter(col("year").isNotNull())
        .groupBy("year", "month")
        .agg(
            count("*").alias("review_count"),
            avg("Score").alias("avg_score"),
            avg("Rating").alias("avg_beauty_rating"),
            avg("score_diff").alias("avg_score_diff")
        )
        .orderBy("year", "month")
    )
    
    print(f"✅ Agrégation créée: {df_monthly.count()} périodes")
    
    return df_monthly

def create_bi_export(df_mart, output_path: str, sample_size: int = 10000):
    """
    Crée les exports BI-ready (Parquet + CSV)
    
    Args:
        df_mart: DataFrame mart
        output_path: Chemin de sortie
        sample_size: Taille de l'échantillon CSV
    """
    print("📤 Création exports BI-ready...")
    
    # Parquet complet
    df_mart.write \
        .mode("overwrite") \
        .parquet(f"{output_path}/bi_ready.parquet")
    
    # CSV léger (échantillon)
    df_mart.limit(sample_size) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_path}/bi_sample.csv")
    
    print(f"✅ Exports créés: {output_path}/bi_ready.parquet et {output_path}/bi_sample.csv")