# src/quality/quality_checks.py
"""
Module de contrôles qualité des données
"""

from pyspark.sql.functions import col, count, when
import datetime

def run_quality_checks(df, run_id: str):
    """
    Exécute 4+ contrôles qualité sur un DataFrame
    
    Args:
        df: DataFrame à vérifier
        run_id: Identifiant d'exécution
        
    Returns:
        DataFrame avec les résultats des checks
    """
    print(f"🔍 Exécution contrôles qualité (Run: {run_id})")
    
    total_count = df.count()
    
    # Check 1: Scores dans plage 1-5
    valid_scores = df.filter(col("Score").between(1, 5)).count()
    score_pct = valid_scores / total_count if total_count > 0 else 0
    
    # Check 2: Pas de UserId nuls
    no_null_users = df.filter(col("UserId").isNotNull()).count()
    user_pct = no_null_users / total_count if total_count > 0 else 0
    
    # Check 3: Complétude des dates
    valid_dates = df.filter(col("ReviewDate").isNotNull()).count()
    date_pct = valid_dates / total_count if total_count > 0 else 0
    
    # Check 4: Pas de doublons sur Id
    duplicate_ids = df.groupBy("Id").count().filter("count > 1").count()
    
    # Check 5: Cohérence ProductId
    valid_products = df.filter(col("ProductId").isNotNull()).count()
    product_pct = valid_products / total_count if total_count > 0 else 0
    
    # Création du rapport
    checks_data = [
        ("score_range_1_5", "PASS" if score_pct >= 0.99 else "FAIL", score_pct, 0.99, run_id),
        ("no_null_userid", "PASS" if user_pct >= 0.99 else "FAIL", user_pct, 0.99, run_id),
        ("valid_dates", "PASS" if date_pct >= 0.95 else "FAIL", date_pct, 0.95, run_id),
        ("no_duplicate_ids", "PASS" if duplicate_ids == 0 else "FAIL", duplicate_ids, 0, run_id),
        ("product_id_completeness", "PASS" if product_pct >= 0.99 else "FAIL", product_pct, 0.99, run_id)
    ]
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    schema = ["check_name", "status", "metric_value", "threshold", "run_id"]
    df_report = spark.createDataFrame(checks_data, schema)
    
    print(f"✅ {len(checks_data)} contrôles qualité exécutés")
    
    return df_report

def save_quality_report(df_report, output_path: str):
    """
    Sauvegarde du rapport qualité
    
    Args:
        df_report: DataFrame du rapport
        output_path: Chemin de sortie
    """
    print(f"💾 Sauvegarde rapport qualité: {output_path}")
    
    df_report.write \
        .mode("append") \
        .parquet(output_path)
    
    print(f"✅ Rapport qualité sauvegardé: {output_path}")