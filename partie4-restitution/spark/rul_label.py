"""
SCRUM-40 — Étiquette « jours restants avant la panne » (RUL).

Lit les relevés SMART nettoyés (une ligne = un disque, un jour) et ajoute :
  - date_panne   : date de la prochaine panne du disque (null s'il ne tombe pas en panne)
  - rul_jours    : jours restants avant cette panne (0 = jour de la panne, où label = 1)
  - rul_plafonne : RUL limité à --plafond jours, pour entraîner un modèle :
        * disque tombé en panne                        -> min(rul_jours, plafond)
        * disque sain encore observé >= plafond jours  -> plafond (il a tenu au moins ça)
        * sinon                                        -> null (on ne sait pas : ligne censurée)

Rappel Backblaze : label = 1 le dernier jour où le disque fonctionne, puis il disparaît.
L'étiquette « panne dans 7 jours » de la partie 2 (SCRUM-34) correspond à rul_jours <= 7.

Exemple (cluster) :
  spark-submit --master spark://spark-master:7077 rul_label.py
Exemple (local, CSV Backblaze) :
  spark-submit rul_label.py --entree data/sample_smart.csv --format csv --sortie out/smart_rul
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from commun import HDFS, lire, session


def ajouter_rul(df: DataFrame, plafond: int = 30) -> DataFrame:
    # Fenêtre « de ce jour jusqu'à la fin de l'historique du disque »
    futur = (Window.partitionBy("device_id").orderBy("date")
             .rowsBetween(Window.currentRow, Window.unboundedFollowing))
    disque = Window.partitionBy("device_id")

    return (
        df
        .withColumn("date_panne", F.min(F.when(F.col("label") == 1, F.col("date"))).over(futur))
        .withColumn("_derniere_obs", F.max("date").over(disque))
        .withColumn("rul_jours", F.datediff("date_panne", "date"))
        .withColumn(
            "rul_plafonne",
            F.when(F.col("rul_jours").isNotNull(), F.least(F.col("rul_jours"), F.lit(plafond)))
             .when(F.datediff("_derniere_obs", "date") >= plafond, F.lit(plafond)),
        )
        .drop("_derniere_obs")
    )


def lire_arguments():
    parser = argparse.ArgumentParser(description="SCRUM-40 : calcul de l'étiquette RUL")
    parser.add_argument("--entree", default=f"{HDFS}/data/smart/clean",
                        help="relevés SMART nettoyés (partie 2)")
    parser.add_argument("--format", choices=["parquet", "csv", "json"], default="parquet")
    parser.add_argument("--sortie", default=f"{HDFS}/data/smart/rul", help="dossier Parquet de sortie")
    parser.add_argument("--plafond", type=int, default=30,
                        help="RUL maximum en jours ; à garder bien plus court que la période couverte")
    return parser.parse_args()


def main():
    args = lire_arguments()
    spark = session("SCRUM-40-rul")

    resultat = ajouter_rul(lire(spark, args.entree, args.format), args.plafond)
    resultat.repartition("date").write.mode("overwrite").partitionBy("date").parquet(args.sortie)

    ecrit = spark.read.parquet(args.sortie)
    r = ecrit.agg(
        F.count("*").alias("lignes"),
        F.countDistinct("device_id").alias("disques"),
        F.countDistinct(F.when(F.col("date_panne").isNotNull(), F.col("device_id"))).alias("disques_en_panne"),
        F.sum(F.col("rul_jours").isNotNull().cast("int")).alias("lignes_avec_rul"),
        F.sum(F.col("rul_plafonne").isNull().cast("int")).alias("lignes_censurees"),
    ).first()
    print("---- Rapport RUL ----")
    for cle, valeur in r.asDict().items():
        print(f"{cle} : {valeur}")
    print(f"écrit dans {args.sortie}")
    (ecrit.where("rul_jours IS NOT NULL")
        .select("device_id", "date", "label", "date_panne", "rul_jours", "rul_plafonne")
        .orderBy("device_id", "date").show(10, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
