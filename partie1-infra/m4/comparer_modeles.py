"""
Comparaison des algorithmes de clustering pour M4 (SCRUM-45).

Les textes nettoyés sont transformés en vecteurs TF-IDF, puis regroupés par
trois algorithmes : K-Means, clustering hiérarchique (agglomératif) et DBSCAN.

Deux critères :
- silhouette : les groupes sont-ils compacts et bien séparés ? (-1 à 1)
- pureté : chaque groupe contient-il une seule famille d'incident ? (0 à 1)
  Calculée avec la colonne `scenario`, qui ne sert qu'à l'évaluation.

Exemple :
    python comparer_modeles.py --entree logs_m4_propres.csv
"""

import argparse

import pandas as pd
from sklearn.cluster import DBSCAN, AgglomerativeClustering, KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import adjusted_rand_score, silhouette_score

GRAINE = 42


def purete(familles, groupes):
    """Part des logs qui appartiennent à la famille majoritaire de leur groupe."""
    tableau = pd.crosstab(groupes, familles)
    return tableau.max(axis=1).sum() / len(familles)


def evaluer(nom, groupes, X, familles):
    valides = groupes != -1                  # DBSCAN marque le bruit avec -1
    nb_groupes = len(set(groupes[valides]))
    silhouette = (silhouette_score(X[valides], groupes[valides], metric="cosine")
                  if nb_groupes > 1 else float("nan"))
    return {
        "modele": nom,
        "groupes": nb_groupes,
        "bruit": int((~valides).sum()),
        "silhouette": round(silhouette, 3),
        "purete": round(purete(familles, groupes), 3),
        "ari": round(adjusted_rand_score(familles, groupes), 3),
    }


def choisir_k(X, k_max):
    """Teste K-Means pour k = 2..k_max et garde le k à la meilleure silhouette.

    Les logs suivent un petit nombre de modèles de messages : la silhouette
    augmente tant que k est inférieur à ce nombre. k_max fixe donc aussi une
    limite de lisibilité (nombre de familles affichées dans Grafana).
    """
    scores = {}
    for k in range(2, k_max + 1):
        groupes = KMeans(n_clusters=k, n_init=10, random_state=GRAINE).fit_predict(X)
        scores[k] = silhouette_score(X, groupes, metric="cosine")
    print("Choix du nombre de groupes (K-Means) :")
    for k, score in scores.items():
        print(f"  k = {k:<2} silhouette = {score:.3f}")
    return max(scores, key=scores.get)


def lire_arguments():
    parser = argparse.ArgumentParser(description="Comparaison des modèles de clustering M4")
    parser.add_argument("--entree", default="logs_m4_propres.csv")
    parser.add_argument("--k-max", type=int, default=10)
    parser.add_argument("--eps", type=float, default=0.5, help="rayon de voisinage de DBSCAN")
    parser.add_argument("--sortie", default="comparaison_m4.csv")
    return parser.parse_args()


def main():
    args = lire_arguments()
    logs = pd.read_csv(args.entree)
    familles = logs["scenario"]

    vectoriseur = TfidfVectorizer()
    X = vectoriseur.fit_transform(logs["texte"])
    print(f"{X.shape[0]} logs, {X.shape[1]} mots dans le vocabulaire TF-IDF\n")

    k = choisir_k(X, args.k_max)
    print(f"-> meilleur k = {k}\n")

    modeles = {
        "K-Means": KMeans(n_clusters=k, n_init=10, random_state=GRAINE),
        "Hiérarchique": AgglomerativeClustering(n_clusters=k, metric="cosine", linkage="average"),
        "DBSCAN": DBSCAN(eps=args.eps, min_samples=5, metric="cosine"),
    }
    resultats = []
    for nom, modele in modeles.items():
        donnees = X.toarray() if nom == "Hiérarchique" else X
        groupes = modele.fit_predict(donnees)
        resultats.append(evaluer(nom, groupes, X, familles))

    tableau = pd.DataFrame(resultats)
    tableau.to_csv(args.sortie, index=False)
    print(tableau.to_string(index=False))
    print(f"\nRésultats enregistrés dans {args.sortie}")


if __name__ == "__main__":
    main()
