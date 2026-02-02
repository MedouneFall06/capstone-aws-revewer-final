
# 📊 PROJET DATA ENGINEERING - LAKEHOUSE EN PYSPARK

## 🎯 CONTEXTE DU PROJET
**Capstone Project** pour le cours de Data Engineering - IPSL DIC3 Informatique  
**Enseignant** : Mbaye Babacar Gueye, Ph.D  
**Objectif** : Construire une plateforme Data Engineering Lakehouse en PySpark local (Batch) avec enrichissement multi-sources

---

## 🏗️ ARCHITECTURE DU PROJET

### Structure des Dossiers


project/
├── data/
│   ├── bronze/
│   │   ├── main/                # Dataset principal (≥ 8 Go)
│   │   └── enrich/              # Source secondaire
│   ├── silver/
│   │   ├── main_clean/          # Données nettoyées
│   │   ├── enrich_clean/        # Source enrichie nettoyée
│   │   └── joined/              # Jointure enrichie
│   └── gold/
│       ├── marts/               # Tables analytiques principales
│       ├── aggregates/          # Agrégations temporelles
│       └── exports/             # Exports BI-ready
├── src/
│   ├── ingestion/               # Scripts d'ingestion
│   ├── transforms/              # Transformations
│   ├── quality/                 # Contrôles qualité
│   └── utils/                   # Utilitaires
├── notebooks/                   # Notebooks Databricks
├── configs/                     # Fichiers de configuration
├── reports/
│   ├── data_quality/            # Rapports qualité
│   └── benchmarks/              # Benchmarks performance
└── livrables/                   # Documents à remettre


---

## 📁 CONTENU DU DÉPÔT

### 1. CODE SOURCE (`/src/`)
- **`ingestion/`** : Chargement des données vers Bronze avec idempotence
- **`transforms/`** : Nettoyage Silver, enrichissement, création Gold
- **`quality/`** : 4+ contrôles qualité avec génération de rapports
- **`utils/`** : Helpers Spark, logging, configuration

### 2. NOTEBOOKS DATABRICKS (`/notebooks/`)
- **`capstone-project-ipsl-dic3.ipynb`** : Notebook principal (exemple du professeur)
- **`Netoyage standardisation et feature engineering.ipynb`** : Notre implémentation

### 3. CONFIGURATION (`/configs/`)
- Paramètres de connexion Databricks
- Schémas de données explicites
- Configuration Spark optimisée

### 4. RAPPORTS (`/reports/`)
- **`data_quality/`** : Résultats des contrôles qualité (Parquet)
- **`benchmarks/`** : Mesures de performance (Parquet)

### 5. LIVRABLES (`/livrables/`)
- **`rapports_techniques/`** :
  - `data_quality_report.md` : Rapport qualité des données
  - `performance_benchmarks.md` : Rapport performance
  - `screenshots/` : Captures d'écran requis
- **`rapport_final.pdf`** : Rapport synthèse (6+ pages)
- **`demo_video.mp4`** : Vidéo de démonstration

---

## 🚀 INSTALLATION ET EXÉCUTION

### Prérequis
- **Databricks** (Unity Catalog activé)
- **Cluster Spark** : Single Node avec ≥ 15GB RAM
- **Python 3.9+** avec PySpark
- **Volume Unity Catalog** configuré

### Configuration Initiale
python
# Variables à modifier dans votre notebook
CATALOG = "workspace"
SCHEMA = "projetbigdata"
VOLUME = "data"
PROJECT = "amazon_reviews"


Exécution du Pipeline

1. Configuration et création dossiers
2. Ingestion Bronze (Amazon Reviews + Beauty Ratings)
3. Transformation Silver (nettoyage)
4. Enrichissement multi-sources (jointure + 2 features)
5. Création Gold Layer (3 outputs)
6. Contrôles qualité (4+ checks)
7. Benchmarks performance

Vérifiez l'architecture :
python
display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{PROJECT}"))


---

## 📊 DATASETS UTILISÉS

**Dataset Principal : Amazon Reviews (≥ 8 Go)**
- Source : Amazon Product Reviews dataset
- Taille : 8.71 Go
- Colonnes : Id, ProductId, UserId, Score, Time, Summary, Text
- Schéma explicite défini dans le code

**Dataset d'Enrichissement : Beauty Ratings**
- Source : Beauty product ratings
- Jointure sur : UserId + ProductId
- Features créées :
  - `score_diff` : Différence entre Score et Rating
  - `is_specialist` : Présence d'un rating beauté

---

## ⚡ OPTIMISATIONS IMPLÉMENTÉES

1. **Broadcast Join**
    python
    df_main.join(broadcast(df_enrich), ["UserId", "ProductId"], "left")
    
2. **Partitionnement Intelligent**
    - Tables Gold partitionnées par colonnes fréquentes
    - `reviews_mart` → partitionné par `has_beauty_rating`
    - `monthly_stats` → partitionné par `year`
3. **Configuration Spark Optimisée**
    python
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    
4. **Format Delta Lake**
    - Transactions ACID
    - Time travel des données
    - Support des opérations MERGE

---

## ✅ EXIGENCES SATISFAITES

**Ingestion & Architecture (15 points)**
- Dataset principal ≥ 8 Go (Amazon Reviews)
- Source secondaire (Beauty Ratings)
- Architecture Bronze/Silver/Gold respectée
- Idempotence avec run_date

**Transformations & Enrichissement (20 points)**
- Schémas explicites définis
- Gestion des nulls, doublons, types
- Jointure réelle Amazon + Beauty
- 2+ features d'enrichissement

**Modélisation Gold (15 points)**
- 1 "mart" principal (`reviews_mart`)
- 1 agrégation temporelle (mensuelle)
- 1 export BI-ready (Parquet + CSV)

**Qualité des Données (15 points)**
- 5 contrôles qualité implémentés
- Rapports générés dans `/reports/data_quality/`
- Table résultats avec `check_name`, `status`, `metric_value`, `threshold`, `run_id`

**Performance & Optimisation (15 points)**
- 4 optimisations documentées
- Benchmarks avant/après
- Mesures : durée pipeline, taille outputs, nb fichiers

**Qualité du Code & Reproductibilité (10 points)**
- Code structuré et commenté
- Variables centralisées
- Exécution idempotente

**Documentation & Soutenance (10 points)**
- README complet
- Rapports techniques générés
- Screenshots Spark UI inclus

**Bonus (+15 points)**
- Observabilité et logs
- Export PostgreSQL (optionnel)

---

## 📈 RÉSULTATS OBTENUS

**Performances**
- Temps total pipeline : ~185 secondes
- Taille Bronze : 8.71 Go
- Gain avec optimisations : ~42%
- Shuffle réduit : 57% moins de données déplacées

**Qualité des Données**
- Score range check : 99.98% dans plage 1-5
- UserID complétude : 100%
- Dates valides : 99.87%
- Score diff cohérence : 99.99% ≤ 4

---

## 👥 ÉQUIPE

Noms des participants :
- Abass AIDARA - aidara.abass@ugb.edu.sn
- Médoune FALL - fall.medoune@ugb.edu.sn
- Lamine THIAM - thiam.lamine@ugb.edu.sn
Encadrant : Mbaye Babacar Gueye, Ph.D

---

## 📋 LIVRABLES À REMETTRE

- ✅ Dépôt Git complet (ce repository)
- 📄 Rapport final (6+ pages dans `/livrables/`)
- 📊 Rapports techniques (qualité + performance dans `/livrables/rapports_techniques/`)
- 🎥 Démonstration (vidéo + logs d'exécution)

---

## 🔧 DÉPANNAGE

**Erreurs Courantes**

- "Unable to infer schema for Parquet"
    python
    # Utilisez un schéma explicite
    schema = StructType([...])
    df = spark.read.schema(schema).parquet(path)
    
- Volume Unity Catalog non trouvé
    python
    # Vérifiez les permissions
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    
- Mémoire insuffisante
    python
    # Optimisez les partitions
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    df = df.coalesce(16)
    

**Support**

Pour toute question ou problème, contactez l'équipe via les emails ci-dessus.

---

## 📄 LICENCE

Ce projet est réalisé dans le cadre académique du cours de Data Engineering IPSL DIC3.  
Le code est fourni à titre éducatif et ne peut être utilisé à des fins commerciales sans autorisation.

---

## 🎓 REMERCIEMENTS

- Professeur : Mbaye Babacar Gueye, Ph.D pour l'encadrement
- Databricks : Pour la plateforme Community Edition

Dernière mise à jour : 2 Février 2026 
Statut du projet : ✅ COMPLET


---
