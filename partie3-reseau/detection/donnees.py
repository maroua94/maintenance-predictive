"""Chargement des données réseau pour M3 (détection d'anomalies)."""

import argparse
import json
from pathlib import Path

import pandas as pd

METRIQUES = [
    "cpu_pct",
    "mem_pct",
    "temperature_c",
    "in_mbps",
    "out_mbps",
    "if_errors",
    "packet_loss_pct",
]
COLONNES = ["device_id", "device_type", "event_time", *METRIQUES, "scenario"]


def _mettre_en_forme(df):
    """Garde les colonnes utiles, dans le même ordre pour les deux sources."""
    df = df[COLONNES].copy()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    return df.sort_values(["device_id", "event_time"]).reset_index(drop=True)


def charger_parquet(dossier, date):
    """Lit une journée du simulateur dans la copie locale de /data/network/clean."""
    chemin = Path(dossier) / "source=simulateur" / f"date={date}"
    return _mettre_en_forme(pd.read_parquet(chemin))


def charger_jsonl(fichier):
    """Lit un fichier produit par le simulateur avec --sortie."""
    with open(fichier, encoding="utf-8") as f:
        messages = [json.loads(ligne) for ligne in f]

    # metrics.cpu_pct devient cpu_pct, meta.scenario devient scenario
    df = pd.json_normalize(messages)
    df = df.rename(columns=lambda nom: nom.split(".")[-1])
    df = df.rename(columns={"timestamp": "event_time"})
    return _mettre_en_forme(df)

def separer(df):
    """Sépare les entrées du modèle et la vérité terrain (1 = en panne)."""
    X = df[METRIQUES]
    y = (df["scenario"] != "normal").astype(int)
    return X, y

def resumer(nom, df):
    print(f"---- {nom} ----")
    print(f"lignes : {len(df)}")
    print(f"équipements : {df['device_id'].nunique()}")
    print(f"période : {df['event_time'].min()} au {df['event_time'].max()}")
    print("relevés par scénario :")
    print(df["scenario"].value_counts().to_string())
    en_panne = df.loc[df["scenario"] != "normal", "device_id"].nunique()
    print(f"équipements en panne : {en_panne}")
    print("valeurs manquantes :", int(df[METRIQUES].isna().sum().sum()))
    print(df[METRIQUES].describe().round(1).to_string())
    print()


def lire_arguments():
    parser = argparse.ArgumentParser(description="Aperçu des données d'entraînement et de test de M3")
    parser.add_argument("--clean", default=str(Path.home() / "donnees-m3" / "clean"),
                        help="copie locale de /data/network/clean")
    parser.add_argument("--date", default="2026-11-26", help="journée d'entraînement")
    parser.add_argument("--test", default=str(Path.home() / "donnees-m3" / "test_graine7.jsonl"),
                        help="fichier de test (graine 7)")
    return parser.parse_args()


def main():
    args = lire_arguments()
    resumer("entraînement (graine 42)", charger_parquet(args.clean, args.date))
    resumer("test (graine 7)", charger_jsonl(args.test))


if __name__ == "__main__":
    main()
