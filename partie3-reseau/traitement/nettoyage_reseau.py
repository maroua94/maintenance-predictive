"""
Nettoyage des données réseau (partie 3).

Lit le topic Kafka network-metrics (routeurs SNMP et simulateur), nettoie les
messages et les écrit en Parquet dans HDFS, partitionnés par source et par date.
Les données nettoyées servent à l'entraînement et à l'évaluation de M3.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Plage de valeurs possibles pour chaque métrique (None = pas de borne)
BORNES = {
    "cpu_pct": (0, 100),
    "mem_pct": (0, 100),
    "temperature_c": (-20, 150),
    "in_mbps": (0, None),
    "out_mbps": (0, None),
    "if_errors": (0, None),
    "if_down": (0, None),
    "packet_loss_pct": (0, 100),
}
METRIQUES_ENTIERES = ("if_errors", "if_down")

SOURCES = ("gns3", "simulateur")

SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("source", StringType()),
    StructField("type", StringType()),
    StructField("device_id", StringType()),
    StructField("device_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("metrics", StructType([StructField(m, DoubleType()) for m in BORNES])),
    StructField("meta", StructType([
        StructField("ip", StringType()),
        StructField("scenario", StringType()),
    ])),
])


def valeur_valide(nom):
    """Garde la métrique si elle est dans sa plage, sinon la remplace par null."""
    colonne = F.col(f"metrics.{nom}")
    mini, maxi = BORNES[nom]
    condition = colonne >= mini
    if maxi is not None:
        condition = condition & (colonne <= maxi)
    valeur = F.when(condition, colonne)
    if nom in METRIQUES_ENTIERES:
        valeur = valeur.cast("int")
    return valeur.alias(nom)


def hors_plage(nom):
    """1 si la métrique est renseignée mais hors de sa plage, sinon 0."""
    colonne = F.col(f"metrics.{nom}")
    mini, maxi = BORNES[nom]
    condition = colonne < mini
    if maxi is not None:
        condition = condition | (colonne > maxi)
    return F.when(colonne.isNotNull() & condition, 1).otherwise(0)


def decoder(textes):
    """Chaînes JSON brutes (colonne « value ») -> une colonne par champ du format commun."""
    return textes.select(F.from_json("value", SCHEMA).alias("m")).select("m.*")


def nettoyer(messages):
    """Supprime les messages invalides et les doublons, neutralise les valeurs impossibles."""
    valides = (
        messages
        .filter(F.col("device_id").isNotNull() & F.col("source").isin(*SOURCES))
        .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX"))
        .filter(F.col("event_time").isNotNull())
    )
    return (
        valides
        .select(
            "device_id",
            "device_type",
            "source",
            "type",
            "event_time",
            *[valeur_valide(m) for m in BORNES],
            F.col("meta.ip").alias("ip"),
            # Vérité terrain du simulateur : pour évaluer M3, jamais comme entrée du modèle
            F.col("meta.scenario").alias("scenario"),
            F.to_date("event_time").alias("date"),
        )
        .dropDuplicates(["device_id", "event_time"])
    )


def lire_arguments():
    parser = argparse.ArgumentParser(description="Nettoyage des données réseau vers HDFS")
    parser.add_argument("--kafka", default="kafka:29092", help="broker Kafka (vu depuis le cluster)")
    parser.add_argument("--topic", default="network-metrics")
    parser.add_argument("--sortie", default="hdfs://namenode:9000/data/network/clean",
                        help="dossier Parquet de sortie")
    return parser.parse_args()


def main():
    args = lire_arguments()
    spark = (
        SparkSession.builder
        .appName("nettoyage-reseau")
        # Réécrit seulement les dates présentes dans ce lot, les plus anciennes restent dans HDFS
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # Peu de données et 4 cœurs : 200 partitions par défaut seraient inutiles
        .config("spark.sql.shuffle.partitions", "8")
        # Le client HDFS de Spark réplique 3 fois par défaut ; le cluster n'a que 2 datanodes
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    brut = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(F.col("value").cast("string").alias("value"))
    )

    messages = decoder(brut).cache()
    propres = nettoyer(messages).cache()

    lus = brut.count()
    decodes = messages.filter(F.col("device_id").isNotNull()).count()
    corriges = messages.select(sum(hors_plage(m) for m in BORNES).alias("n")).agg(F.sum("n")).first()[0] or 0
    ecrits = propres.count()

    (
        # Un seul fichier Parquet par dossier source/date, plutôt que des dizaines de petits fichiers
        propres.repartition("source", "date")
        .write
        .mode("overwrite")
        .partitionBy("source", "date")
        .parquet(args.sortie)
    )

    print("---- Rapport de nettoyage ----")
    print(f"messages lus dans Kafka : {lus}")
    print(f"JSON invalides : {lus - decodes}")
    print(f"doublons, dates ou sources invalides : {decodes - ecrits}")
    print(f"valeurs hors plage remplacées par null : {corriges}")
    print(f"lignes écrites : {ecrits} dans {args.sortie}")
    propres.groupBy("source").count().orderBy("source").show()

    spark.stop()


if __name__ == "__main__":
    main()
