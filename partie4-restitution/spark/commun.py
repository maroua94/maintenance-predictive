"""
Fonctions communes aux jobs Spark de la partie 4 (lecture, format commun, session).

Les jobs acceptent trois formes de données SMART et les ramènent toutes au format
commun du projet (docs/format_json.md), aplati :
    device_id, date, label, model, smart_*_raw
  - Parquet nettoyé de la partie 2 (HDFS)        -> déjà proche, juste vérifié
  - messages Kafka décodés (metrics.*, meta.*)   -> aplatis
  - CSV Backblaze bruts (serial_number, failure)  -> renommés
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"

RENOMMAGES_BACKBLAZE = {"serial_number": "device_id", "failure": "label"}


def session(nom: str) -> SparkSession:
    """Même configuration que les jobs des autres parties (cluster 2 workers, UTC)."""
    spark = (
        SparkSession.builder
        .appName(nom)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normaliser(df: DataFrame) -> DataFrame:
    if "metrics" in df.columns:
        df = df.select("*", "metrics.*").drop("metrics")
    if "meta" in df.columns:
        champs = [c for c in df.schema["meta"].dataType.fieldNames() if c not in df.columns]
        df = df.select("*", *[F.col(f"meta.{c}").alias(c) for c in champs]).drop("meta")
    for ancien, nouveau in RENOMMAGES_BACKBLAZE.items():
        if ancien in df.columns and nouveau not in df.columns:
            df = df.withColumnRenamed(ancien, nouveau)

    if "date" in df.columns:
        date = F.to_date("date")
    elif "event_time" in df.columns:
        date = F.to_date("event_time")
    elif "timestamp" in df.columns:
        date = F.to_date(F.to_timestamp("timestamp"))
    else:
        raise ValueError("Aucune colonne de date (date, event_time ou timestamp)")
    manquantes = {"device_id", "label"} - set(df.columns)
    if manquantes:
        raise ValueError(f"Colonnes manquantes : {sorted(manquantes)} (colonnes lues : {df.columns})")

    return (df.withColumn("date", date)
              .withColumn("label", F.coalesce(F.col("label").cast("int"), F.lit(0))))


def lire(spark: SparkSession, chemin: str, fmt: str = "parquet") -> DataFrame:
    if fmt == "csv":
        df = spark.read.option("header", True).option("inferSchema", True).csv(chemin)
    elif fmt == "json":
        df = spark.read.json(chemin)
    else:
        df = spark.read.parquet(chemin)
    return normaliser(df)
