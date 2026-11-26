"""Comparaison de trois modèles de détection d'anomalies réseau (M3)."""

import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.neighbors import LocalOutlierFactor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM

from donnees import charger_parquet, decouper, separer

GRAINE = 42


def creer_modeles():
    return {
        "Isolation Forest": IsolationForest(n_estimators=200, random_state=GRAINE),
        "One-Class SVM": OneClassSVM(nu=0.05, gamma="scale"),
        "LOF": LocalOutlierFactor(n_neighbors=20, novelty=True),
    }


def alertes_persistantes(df, alerte, nb_releves):
    """Vrai quand un équipement est en alerte sur nb_releves relevés consécutifs."""
    serie = pd.Series(alerte, index=df.index)
    glissant = serie.groupby(df["device_id"]).transform(lambda x: x.rolling(nb_releves).sum())
    return glissant >= nb_releves


def evaluer_equipements(df, persistante):
    """Pannes détectées, délai de détection et équipements normaux en fausse alerte."""
    detectees, delais, fausses = 0, [], 0
    for _, releves in df.assign(persistante=persistante).groupby("device_id"):
        en_panne = releves["scenario"] != "normal"
        if en_panne.any():
            debut = releves.loc[en_panne, "event_time"].min()
            alertes = releves.loc[en_panne & releves["persistante"], "event_time"]
            if not alertes.empty:
                detectees += 1
                delais.append((alertes.min() - debut).total_seconds() / 60)
        elif releves["persistante"].any():
            fausses += 1

    nb_pannes = df.loc[df["scenario"] != "normal", "device_id"].nunique()
    nb_normaux = df["device_id"].nunique() - nb_pannes
    return {
        "pannes détectées": f"{detectees}/{nb_pannes}",
        "délai médian (min)": round(float(np.median(delais)), 1) if delais else None,
        "fausses alertes": f"{fausses}/{nb_normaux}",
    }


def comparer(entrainement, test, taille_echantillon, quantile, nb_releves):
    X_train, _ = separer(entrainement)
    X_test, y_test = separer(test)

    # One-Class SVM devient très lent au-delà de quelques dizaines de milliers de lignes
    echantillon = X_train.sample(taille_echantillon, random_state=GRAINE)

    resultats = []
    for nom, modele in creer_modeles().items():
        pipeline = make_pipeline(StandardScaler(), modele)
        debut = time.perf_counter()
        pipeline.fit(echantillon)
        duree = time.perf_counter() - debut

        # score_samples est élevé pour un relevé normal, on l'inverse
        seuil = np.quantile(-pipeline.score_samples(echantillon), quantile)
        score_test = -pipeline.score_samples(X_test)
        alerte = (score_test > seuil).astype(int)

        ligne = {
            "modèle": nom,
            "ROC AUC": round(roc_auc_score(y_test, score_test), 3),
            "PR AUC": round(average_precision_score(y_test, score_test), 3),
        }
        ligne.update(evaluer_equipements(test, alertes_persistantes(test, alerte, nb_releves)))
        ligne["entraînement (s)"] = round(duree, 1)
        resultats.append(ligne)

    return pd.DataFrame(resultats)


def lire_arguments():
    dossier = Path.home() / "donnees-m3"
    parser = argparse.ArgumentParser(description="Comparaison des modèles de M3")
    parser.add_argument("--clean", default=str(dossier / "clean"), help="copie locale de /data/network/clean")
    parser.add_argument("--date", default="2026-11-26", help="journée à utiliser")
    parser.add_argument("--echantillon", type=int, default=20000, help="lignes utilisées pour l'entraînement")
    parser.add_argument("--quantile", type=float, default=0.95, help="seuil d'alerte sur les scores d'entraînement")
    parser.add_argument("--persistance", type=int, default=3, help="relevés consécutifs avant une alerte")
    parser.add_argument("--sortie", default=str(dossier / "comparaison_m3.csv"), help="tableau des résultats")
    return parser.parse_args()


def main():
    args = lire_arguments()
    entrainement, test = decouper(charger_parquet(args.clean, args.date))

    tableau = comparer(entrainement, test, args.echantillon, args.quantile, args.persistance)
    print(tableau.to_string(index=False))
    tableau.to_csv(args.sortie, index=False)
    print(f"\nrésultats enregistrés dans {args.sortie}")


if __name__ == "__main__":
    main()
