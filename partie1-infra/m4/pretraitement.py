"""
Prétraitement du texte des logs syslog pour M4 (SCRUM-44).

Ne garde que le sens d'un message en retirant tout ce qui varie d'un
équipement à l'autre (adresses IP, interfaces, utilisateurs, nombres).
Deux logs qui décrivent le même incident donnent alors le même texte.

La fonction `nettoyer` est aussi utilisée par le job streaming (SCRUM-56),
pour traiter les nouveaux logs exactement comme les logs d'entraînement.

Exemple :
    python pretraitement.py --entree logs_m4.csv --sortie logs_m4_propres.csv
"""

import argparse
import csv
import re

# Ordre important : on remplace d'abord les motifs les plus précis
REGLES = [
    (re.compile(r"%(\w+)-\d-(\w+):"), r"\1 \2 "),                  # en-tête Cisco, sans la gravité
    (re.compile(r"\b\d{1,3}(\.\d{1,3}){3}\b"), " ip "),             # adresses IP
    (re.compile(r"\b(gigabit|fast)?ethernet\d+(/\d+)*\b", re.I), " interface "),
    (re.compile(r"\[user: [^\]]+\]|user '[^']+'", re.I), " user "),  # nom d'utilisateur
    (re.compile(r"\bby \w+ on\b", re.I), " by user on "),
    (re.compile(r"\bR\d+\b"), " routeur "),                         # nom du routeur
    (re.compile(r"[^a-zA-Z_ ]"), " "),                              # chiffres et ponctuation
    (re.compile(r"_"), " "),
]

MOTS_VIDES = {"a", "an", "the", "on", "of", "to", "for", "by", "is", "from",
              "and", "after", "than", "more", "using", "with", "host", "process"}


def nettoyer(message):
    """Transforme un log brut en suite de mots significatifs, en minuscules."""
    texte = message
    for motif, remplacement in REGLES:
        texte = motif.sub(remplacement, texte)
    mots = [m for m in texte.lower().split() if m not in MOTS_VIDES and len(m) > 1]
    return " ".join(mots)


def etat(message):
    """Indique si le log signale une panne (down) ou un rétablissement (up)."""
    texte = message.lower()
    if re.search(r"to down|failed|dead timer|restarted|cold start", texte):
        return "down"
    if re.search(r"to up|to full|loading done", texte):
        return "up"
    return ""


def lire_arguments():
    parser = argparse.ArgumentParser(description="Nettoyage du texte des logs syslog")
    parser.add_argument("--entree", default="logs_m4.csv")
    parser.add_argument("--sortie", default="logs_m4_propres.csv")
    return parser.parse_args()


def main():
    args = lire_arguments()
    with open(args.entree, encoding="utf-8") as fichier:
        lignes = list(csv.DictReader(fichier))

    for ligne in lignes:
        ligne["texte"] = nettoyer(ligne["message"])
        ligne["etat"] = etat(ligne["message"])

    with open(args.sortie, "w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=list(lignes[0]))
        writer.writeheader()
        writer.writerows(lignes)

    bruts = len({ligne["message"] for ligne in lignes})
    propres = len({ligne["texte"] for ligne in lignes})
    print(f"{len(lignes)} logs nettoyés dans {args.sortie}")
    print(f"messages différents : {bruts} avant, {propres} après nettoyage")


if __name__ == "__main__":
    main()
