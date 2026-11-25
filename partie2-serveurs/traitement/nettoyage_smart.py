"""
Nettoyage des données SMART Backblaze (partie 2).

Lit les relevés Backblaze (CSV bruts ou Parquet), garde le modèle de disque
retenu, nettoie les valeurs et ajoute l'étiquette « panne dans 7 jours ».
Le résultat (Parquet) sert à M1 (partie 2) et au calcul du RUL de M2 (partie 4).

Colonnes de sortie : device_id, date, model, label, label_7j, smart_*_raw
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SMART = ["smart_5_raw", "smart_9_raw", "smart_187_raw",
         "smart_194_raw", "smart_197_raw", "smart_198_raw"]

# Plage plausible de chaque attribut (None = pas de borne haute)
BORNES = {
    "smart_5_raw": (0, None),
    "smart_9_raw": (0, None),
    "smart_187_raw": (0, None),
    "smart_194_raw": (0, 80),   # température en °C
    "smart_197_raw": (0, None),
    "smart_198_raw": (0, None),
}

HORIZON = 7  # jours : label_7j = 1 de J-6 à J (J = jour de la panne)


def lire(spark, chemin, fmt):
    if fmt == "csv":
        # Pas d'inferSchema : on ne garde que 10 colonnes sur ~190, on les type nous-mêmes
        df = spark.read.option("header", True).csv(chemin)
    else:
        df = spark.read.parquet(chemin)
    return df.select(
        F.col("serial_number").alias("device_id"),
        F.to_date("date").alias("date"),
        F.col("model"),
        F.col("failure").cast("int").alias("label"),
        *[F.col(c).cast("double").alias(c) for c in SMART],
    )


def borner(nom):
    """Remplace par null une valeur hors de sa plage (erreur de capteur)."""
    mini, maxi = BORNES[nom]
    condition = F.col(nom) >= mini
    if maxi is not None:
        condition = condition & (F.col(nom) <= maxi)
    return F.when(condition, F.col(nom)).alias(nom)


def nettoyer(df):
    passe = Window.partitionBy("device_id").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df = (
        df.filter(F.col("device_id").isNotNull() & F.col("date").isNotNull())
        .dropDuplicates(["device_id", "date"])
        .select("device_id", "date", "model",
                F.coalesce("label", F.lit(0)).alias("label"),
                *[borner(c) for c in SMART])
    )
    # Valeur manquante : on reprend la dernière valeur connue du même disque
    for c in SMART:
        df = df.withColumn(c, F.last(c, ignorenulls=True).over(passe))
    # Premiers jours d'un disque sans aucune mesure : rien à reprendre
    return df.dropna(subset=SMART)


def etiqueter(df):
    futur = Window.partitionBy("device_id").orderBy("date") \
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    disque = Window.partitionBy("device_id")

    df = (
        df.withColumn("date_panne", F.min(F.when(F.col("label") == 1, F.col("date"))).over(futur))
        .withColumn("jours_avant_panne", F.datediff("date_panne", "date"))
        .withColumn("derniere_obs", F.max("date").over(disque))
        .withColumn("label_7j", F.when(F.col("jours_avant_panne") < HORIZON, 1).otherwise(0))
    )
    # Disque sans panne à venir : ses 6 derniers jours observés sont incertains
    incertain = F.col("date_panne").isNull() & (F.datediff("derniere_obs", "date") < HORIZON - 1)
    return (df.filter(~incertain)
              .drop("date_panne", "jours_avant_panne", "derniere_obs"))


def lire_arguments():
    parser = argparse.ArgumentParser(description="Nettoyage SMART + étiquette « panne dans 7 jours »")
    parser.add_argument("--entree", default="hdfs://namenode:9000/data/smart/raw",
                        help="CSV Backblaze ou Parquet brut (accepte les jokers *)")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet")
    parser.add_argument("--sortie", default="hdfs://namenode:9000/data/smart/clean",
                        help="dossier Parquet de sortie")
    parser.add_argument("--modeles", default="ST12000NM0008",
                        help="modèles de disque à garder, séparés par des virgules")
    return parser.parse_args()


def main():
    args = lire_arguments()
    spark = (
        SparkSession.builder
        .appName("nettoyage-smart")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    modeles = [m.strip() for m in args.modeles.split(",")]
    brut = lire(spark, args.entree, args.format).filter(F.col("model").isin(*modeles)).cache()
    propre = etiqueter(nettoyer(brut)).cache()

    lus = brut.count()
    ecrits = propre.count()
    propre.orderBy("device_id", "date").write.mode("overwrite").parquet(args.sortie)

    print("---- Rapport de nettoyage SMART ----")
    print(f"modèles gardés : {', '.join(modeles)}")
    print(f"lignes lues : {lus}")
    print(f"lignes supprimées (doublons, sans mesure, fins incertaines) : {lus - ecrits}")
    print(f"lignes écrites : {ecrits} dans {args.sortie}")
    print(f"disques : {propre.select('device_id').distinct().count()}")
    print(f"pannes (label = 1) : {propre.filter('label = 1').count()}")
    propre.groupBy("label_7j").count().orderBy("label_7j").show()

    spark.stop()


if __name__ == "__main__":
    main()