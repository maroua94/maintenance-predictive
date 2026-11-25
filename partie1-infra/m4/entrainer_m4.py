"""
Entraînement final de M4 : nommage des familles d'incidents et sauvegarde (SCRUM-46).

Entraîne TF-IDF + K-Means (modèle retenu en SCRUM-45), affiche le contenu de
chaque groupe, lui donne un nom de famille à partir de ses mots les plus
représentatifs, puis sauvegarde le tout dans un seul fichier.

Exemple :
    python entrainer_m4.py --entree logs_m4_propres.csv --k 10
"""

import argparse

import joblib
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

from pretraitement import nettoyer

GRAINE = 42

# Mots-clés qui identifient chaque famille (vérifiés à la main sur les groupes)
FAMILLES = [
    ("Voisinage OSPF", {"adjchg"}),
    ("Interface / lien", {"updown"}),
    ("Redémarrage", {"restart", "coldstart", "boottime"}),
    ("Configuration", {"configured"}),
    ("Sécurité / connexion", {"login", "userauth"}),
    ("Ressources CPU / mémoire", {"cpuhog", "mallocfail"}),
]

# Correspondance avec la colonne `scenario` du générateur, pour l'évaluation
SCENARIOS = {
    "Voisinage OSPF": "ospf",
    "Interface / lien": "interface",
    "Redémarrage": "redemarrage",
    "Configuration": "configuration",
    "Sécurité / connexion": "securite",
    "Ressources CPU / mémoire": "ressources",
}


def mots_principaux(vectoriseur, modele, nombre=10):
    """Mots qui pèsent le plus dans le centre de chaque groupe (poids non nul)."""
    vocabulaire = vectoriseur.get_feature_names_out()
    resultat = []
    for centre in modele.cluster_centers_:
        ordre = centre.argsort()[::-1][:nombre]
        resultat.append([vocabulaire[i] for i in ordre if centre[i] > 0])
    return resultat


def nommer(mots):
    """Famille du mot-clé le plus important du groupe."""
    for mot in mots:
        for nom, cles in FAMILLES:
            if mot in cles:
                return nom
    return "Autre"


def classer(modele_m4, message):
    """Renvoie la famille d'incident d'un log brut."""
    texte = nettoyer(message)
    X = modele_m4["vectoriseur"].transform([texte])
    groupe = int(modele_m4["kmeans"].predict(X)[0])
    return modele_m4["noms"][groupe]


def lire_arguments():
    parser = argparse.ArgumentParser(description="Entraînement et sauvegarde de M4")
    parser.add_argument("--entree", default="logs_m4_propres.csv")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--sortie", default="modele_m4.joblib")
    return parser.parse_args()


def main():
    args = lire_arguments()
    logs = pd.read_csv(args.entree)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)

    vectoriseur = TfidfVectorizer()
    X = vectoriseur.fit_transform(logs["texte"])
    kmeans = KMeans(n_clusters=args.k, n_init=10, random_state=GRAINE).fit(X)
    logs["groupe"] = kmeans.labels_

    mots = mots_principaux(vectoriseur, kmeans)
    noms = {g: nommer(m) for g, m in enumerate(mots)}

    for g in range(args.k):
        membres = logs[logs["groupe"] == g]
        print(f"Groupe {g} -> {noms[g]}  ({len(membres)} logs)")
        print(f"  mots principaux : {', '.join(mots[g][:6])}")
        print(f"  exemple : {membres['message'].iloc[0]}")

    logs["famille"] = logs["groupe"].map(noms)
    print("\nFamille prédite / famille réelle :")
    print(pd.crosstab(logs["famille"], logs["scenario"]))
    justes = (logs["famille"].map(SCENARIOS) == logs["scenario"]).sum()
    print(f"logs dans la bonne famille : {justes}/{len(logs)} ({justes / len(logs):.1%})")

    joblib.dump({"vectoriseur": vectoriseur, "kmeans": kmeans, "noms": noms}, args.sortie)
    print(f"\nModèle sauvegardé dans {args.sortie}")

    modele_m4 = joblib.load(args.sortie)
    print("\nTest sur de nouveaux logs :")
    for message in [
        "%LINK-3-UPDOWN: Interface GigabitEthernet0/3, changed state to down",
        "%OSPF-5-ADJCHG: Process 1, Nbr 9.9.9.9 on GigabitEthernet0/1 from FULL to DOWN, "
        "Neighbor Down: Dead timer expired",
        "%SEC_LOGIN-4-LOGIN_FAILED: Login failed [user: guest] [Source: 10.0.0.7] "
        "[localport: 23] [Reason: Login Authentication Failed]",
    ]:
        print(f"  {classer(modele_m4, message):<26} <- {message[:60]}...")


if __name__ == "__main__":
    main()
