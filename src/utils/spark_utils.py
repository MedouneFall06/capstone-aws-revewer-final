# src/utils/spark_utils.py
"""
Utilitaires Spark pour la configuration et l'optimisation
"""

def configure_spark_optimizations():
    """
    Configure les optimisations Spark recommandées
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    print("⚡ Configuration optimisations Spark...")
    
    # Optimisation des partitions shuffle
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    
    # Optimisation Delta Lake
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    
    # Optimisation mémoire
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    
    print("✅ Configuration Spark optimisée")
    
    return spark

def calculate_folder_size(path: str) -> float:
    """
    Calcule la taille d'un dossier en Go
    
    Args:
        path: Chemin du dossier
        
    Returns:
        Taille en Go
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    try:
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
        
        total_size = sum(f.getLen() for f in files if not f.isDirectory())
        size_gb = total_size / (1024**3)
        
        return size_gb
    except:
        return 0.0