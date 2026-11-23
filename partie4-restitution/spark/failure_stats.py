"""
SCRUM-39 — Statistiques de pannes avec Spark SQL, écrites dans InfluxDB pour Grafana.

Entrée : sortie de rul_label.py (SCRUM-40). Fonctionne aussi sur les données nettoyées
         sans RUL : la statistique « SMART avant la panne » est alors sautée.

Measurements InfluxDB (lus par grafana/dashboards/maintenance-predictive.json) :
  kpi                disques suivis, pannes, jours-disque, AFR global, âge moyen à la panne
  pannes_jour        par jour : disques suivis, pannes, taux de panne
  afr_modele         taux de panne annualisé par modèle de disque
  smart_reference    moyenne de chaque attribut SMART : disques sains vs tombés en panne
  smart_non_nul      % de disques dont le compteur d'erreurs est > 0 au dernier relevé
  smart_avant_panne  moyenne des compteurs d'erreurs de J-30 à J-0 (jour de la panne)

AFR (formule Backblaze) = pannes / (jours-disque / 365) x 100

L'écriture dans InfluxDB passe par son API HTTP (bibliothèque standard Python) :
rien à installer dans les conteneurs Spark.

Exemple (cluster) :
  spark-submit --master spark://spark-master:7077 failure_stats.py --influx-token <token>
Exemple (test sans InfluxDB) :
  spark-submit failure_stats.py --entree out/smart_rul --no-influx --sortie-stats out/stats
"""

import argparse
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, time, timezone

from pyspark.sql import DataFrame, SparkSession

from commun import HDFS, lire, session

# Attributs retenus par la partie 2 (exploration SCRUM-14)
COMPTEURS = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]
CONTEXTE = ["smart_9_raw", "smart_194_raw"]
LIBELLES = {
    "smart_5_raw": "5 · secteurs réalloués",
    "smart_187_raw": "187 · erreurs non corrigibles",
    "smart_197_raw": "197 · secteurs en attente",
    "smart_198_raw": "198 · secteurs non corrigibles",
    "smart_9_raw": "9 · heures de fonctionnement",
    "smart_194_raw": "194 · température (°C)",
}

FABRICANT_SQL = """
  CASE
    WHEN upper(model) LIKE 'ST%' OR upper(model) LIKE 'SEAGATE%'    THEN 'Seagate'
    WHEN upper(model) LIKE 'HGST%' OR upper(model) LIKE 'HITACHI%'  THEN 'HGST'
    WHEN upper(model) LIKE 'WDC%' OR upper(model) LIKE 'WD%'        THEN 'Western Digital'
    WHEN upper(model) LIKE 'TOSHIBA%'                               THEN 'Toshiba'
    ELSE 'Autre'
  END"""


def calculer_stats(spark: SparkSession, df: DataFrame, min_jours_disque: int, fenetre_rul: int) -> dict:
    df.createOrReplaceTempView("smart")
    colonnes = set(df.columns)
    compteurs = [c for c in COMPTEURS if c in colonnes]
    suivis = compteurs + [c for c in CONTEXTE if c in colonnes]
    age = ("ROUND(AVG(CASE WHEN label = 1 THEN smart_9_raw END) / 24 / 365, 2)"
           if "smart_9_raw" in colonnes else "CAST(NULL AS DOUBLE)")

    stats = {}
    stats["kpi"] = spark.sql(f"""
        SELECT COUNT(DISTINCT device_id)                       AS nb_disques,
               SUM(label)                                      AS nb_pannes,
               COUNT(*)                                        AS jours_disque,
               ROUND(100 * SUM(label) / (COUNT(*) / 365), 2)   AS afr_pct,
               {age}                                           AS age_moyen_panne_ans,
               MIN(date) AS debut,
               MAX(date) AS fin
        FROM smart""")

    stats["pannes_jour"] = spark.sql("""
        SELECT date,
               COUNT(DISTINCT device_id)                                AS nb_disques,
               SUM(label)                                               AS nb_pannes,
               ROUND(100 * SUM(label) / COUNT(DISTINCT device_id), 4)   AS taux_pannes_pct
        FROM smart GROUP BY date ORDER BY date""")

    if "model" in colonnes:
        stats["afr_modele"] = spark.sql(f"""
            SELECT model AS modele, {FABRICANT_SQL} AS fabricant,
                   COUNT(DISTINCT device_id)                       AS nb_disques,
                   COUNT(*)                                        AS jours_disque,
                   SUM(label)                                      AS nb_pannes,
                   ROUND(100 * SUM(label) / (COUNT(*) / 365), 2)   AS afr_pct
            FROM smart GROUP BY model
            HAVING COUNT(*) >= {min_jours_disque}
            ORDER BY afr_pct DESC""")

    if suivis:
        # groupe « panne » = toutes les lignes des disques qui finissent par tomber en panne
        spark.sql("""
            SELECT s.*,
                   CASE WHEN p.device_id IS NULL THEN 'sain' ELSE 'panne' END AS groupe,
                   ROW_NUMBER() OVER (PARTITION BY s.device_id ORDER BY s.date DESC) AS rang
            FROM smart s
            LEFT JOIN (SELECT DISTINCT device_id FROM smart WHERE label = 1) p
                   ON s.device_id = p.device_id""").createOrReplaceTempView("smart_groupe")
        stats["smart_reference"] = spark.sql(
            "SELECT groupe, COUNT(*) AS nb_obs, "
            + ", ".join(f"ROUND(AVG({c}), 3) AS {c}" for c in suivis)
            + " FROM smart_groupe GROUP BY groupe ORDER BY groupe")

    if compteurs:
        stats["smart_non_nul"] = spark.sql(
            "SELECT groupe, COUNT(*) AS nb_disques, "
            + ", ".join(f"ROUND(100 * AVG(CASE WHEN {c} > 0 THEN 1.0 ELSE 0.0 END), 1) AS {c}"
                        for c in compteurs)
            + " FROM smart_groupe WHERE rang = 1 GROUP BY groupe ORDER BY groupe")
        if "rul_jours" in colonnes:
            stats["smart_avant_panne"] = spark.sql(
                "SELECT rul_jours, COUNT(*) AS nb_obs, "
                + ", ".join(f"ROUND(AVG({c}), 3) AS {c}" for c in compteurs)
                + f" FROM smart WHERE rul_jours BETWEEN 0 AND {fenetre_rul}"
                  " GROUP BY rul_jours ORDER BY rul_jours DESC")
    return stats


# ---------------------------------------------------------------- InfluxDB (line protocol)

def _echapper(texte) -> str:
    return (str(texte).replace("\\", "\\\\").replace(",", "\\,")
            .replace("=", "\\=").replace(" ", "\\ "))


def _valeur(v):
    """int -> 12i ; décimal -> 12.5 ; None / NaN -> None (champ ignoré)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return f"{int(v)}i"
    if isinstance(v, int):
        return f"{v}i"
    v = float(v)
    return repr(v) if math.isfinite(v) else None


def ligne(measurement: str, tags: dict, champs: dict, horodatage: datetime):
    valeurs = [(k, _valeur(v)) for k, v in champs.items()]
    valeurs = [f"{_echapper(k)}={v}" for k, v in valeurs if v is not None]
    if not valeurs:
        return None
    etiquettes = "".join(f",{_echapper(k)}={_echapper(v)}"
                         for k, v in sorted(tags.items()) if v not in (None, ""))
    return f"{_echapper(measurement)}{etiquettes} {','.join(valeurs)} {int(horodatage.timestamp())}"


def _minuit(d) -> datetime:
    return datetime.combine(d, time.min, tzinfo=timezone.utc)


def vers_lignes(stats: dict) -> list:
    kpi = stats["kpi"].first().asDict()
    # Les stats « instantanées » sont datées du dernier jour des données : relancer le job
    # sur les mêmes données écrase les points au lieu de les dupliquer.
    t_fin = _minuit(kpi.pop("fin"))
    kpi.pop("debut")
    lignes = [ligne("kpi", {}, kpi, t_fin)]

    for r in stats["pannes_jour"].collect():
        d = r.asDict()
        jour = _minuit(d.pop("date"))
        lignes.append(ligne("pannes_jour", {}, d, jour))

    if "afr_modele" in stats:
        for r in stats["afr_modele"].collect():
            d = r.asDict()
            tags = {"modele": d.pop("modele"), "fabricant": d.pop("fabricant")}
            lignes.append(ligne("afr_modele", tags, d, t_fin))

    # Une ligne par (attribut, groupe) : Grafana pivote ensuite sur le groupe
    for nom in ("smart_reference", "smart_non_nul"):
        if nom not in stats:
            continue
        for r in stats[nom].collect():
            d = r.asDict()
            groupe = d.pop("groupe")
            d.pop("nb_obs", None)
            d.pop("nb_disques", None)
            for attribut, v in d.items():
                lignes.append(ligne(nom, {"attribut": LIBELLES.get(attribut, attribut), "groupe": groupe},
                                    {"valeur": v}, t_fin))

    if "smart_avant_panne" in stats:
        for r in stats["smart_avant_panne"].collect():
            d = r.asDict()
            lignes.append(ligne("smart_avant_panne", {"rul_jours": d.pop("rul_jours")}, d, t_fin))
    return [x for x in lignes if x is not None]


def ecrire_influx(lignes: list, url: str, token: str, org: str, bucket: str) -> None:
    requete = urllib.parse.urlencode({"org": org, "bucket": bucket, "precision": "s"})
    adresse = f"{url.rstrip('/')}/api/v2/write?{requete}"
    for i in range(0, len(lignes), 5000):
        lot = "\n".join(lignes[i:i + 5000]).encode("utf-8")
        req = urllib.request.Request(adresse, data=lot, method="POST", headers={
            "Authorization": f"Token {token}",
            "Content-Type": "text/plain; charset=utf-8",
        })
        try:
            with urllib.request.urlopen(req, timeout=30):
                pass
        except urllib.error.HTTPError as e:
            raise SystemExit(f"InfluxDB a refusé l'écriture ({e.code}) : {e.read().decode(errors='replace')}")
    print(f"{len(lignes)} points écrits dans InfluxDB ({url}, bucket {bucket})")


# ---------------------------------------------------------------- main

def lire_arguments():
    parser = argparse.ArgumentParser(description="SCRUM-39 : statistiques de pannes (Spark SQL -> InfluxDB)")
    parser.add_argument("--entree", default=f"{HDFS}/data/smart/rul", help="sortie de rul_label.py")
    parser.add_argument("--format", choices=["parquet", "csv", "json"], default="parquet")
    parser.add_argument("--sortie-stats", help="dossier (HDFS ou local) où exporter chaque statistique en CSV")
    parser.add_argument("--min-jours-disque", type=int, default=1000,
                        help="jours-disque minimum pour qu'un modèle apparaisse dans afr_modele")
    parser.add_argument("--fenetre-rul", type=int, default=30, help="jours avant la panne étudiés")
    parser.add_argument("--influx-url", default=os.getenv("INFLUXDB_URL", "http://influxdb:8086"))
    parser.add_argument("--influx-token", default=os.getenv("INFLUXDB_TOKEN"))
    parser.add_argument("--influx-org", default=os.getenv("INFLUXDB_ORG", "ensa"))
    parser.add_argument("--influx-bucket", default=os.getenv("INFLUXDB_BUCKET", "maintenance"))
    parser.add_argument("--no-influx", action="store_true", help="calculer sans écrire dans InfluxDB")
    parser.add_argument("--line-protocol", help="fichier local où écrire les points (contrôle)")
    return parser.parse_args()


def main():
    args = lire_arguments()
    spark = session("SCRUM-39-stats-pannes")

    stats = calculer_stats(spark, lire(spark, args.entree, args.format),
                           args.min_jours_disque, args.fenetre_rul)
    for nom, df in stats.items():
        print(f"---- {nom} ----")
        df.show(15, truncate=False)
        if args.sortie_stats:
            df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.sortie_stats}/{nom}")

    lignes = vers_lignes(stats)
    if args.line_protocol:
        with open(args.line_protocol, "w", encoding="utf-8") as f:
            f.write("\n".join(lignes) + "\n")
        print(f"{len(lignes)} points -> {args.line_protocol}")
    if not args.no_influx:
        if not args.influx_token:
            raise SystemExit("Token InfluxDB manquant : --influx-token ou variable INFLUXDB_TOKEN")
        ecrire_influx(lignes, args.influx_url, args.influx_token, args.influx_org, args.influx_bucket)
    spark.stop()


if __name__ == "__main__":
    main()
