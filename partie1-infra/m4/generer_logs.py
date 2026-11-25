"""
Générateur de logs syslog Cisco pour l'entraînement de M4 (SCRUM-43).

Produit des messages réalistes répartis en 6 familles d'incidents, au même
format que le collecteur syslog (docs/format_json.md). La colonne `scenario`
indique la famille d'origine : elle sert uniquement à évaluer le clustering,
jamais à l'entraîner.

Exemples :
    python generer_logs.py --nombre 600 --sortie logs_m4.csv
    python generer_logs.py --format jsonl --sortie logs_m4.jsonl
"""

import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone

ROUTEURS = {"R1": "192.168.10.1", "R2": "192.168.10.2", "R3": "192.168.10.3"}
VOISINS = {"R1": "1.1.1.1", "R2": "2.2.2.2", "R3": "3.3.3.3"}
INTERFACES = ["GigabitEthernet0/0", "GigabitEthernet0/1", "GigabitEthernet0/2",
              "FastEthernet0/0", "FastEthernet0/1"]
UTILISATEURS = ["admin", "root", "cisco", "test", "operator"]
PROCESSUS = ["OSPF Router", "IP Input", "SNMP ENGINE", "Exec", "ARP Input"]

# Part de chaque famille dans le jeu de données
REPARTITION = {
    "interface": 0.25,
    "ospf": 0.20,
    "configuration": 0.20,
    "securite": 0.15,
    "redemarrage": 0.10,
    "ressources": 0.10,
}


def ip_admin():
    return f"192.168.10.{random.randint(20, 120)}"


def interface(routeur):
    nom = random.choice(INTERFACES)
    etat = random.choice(["down", "up"])
    if random.random() < 0.5:
        return f"%LINK-3-UPDOWN: Interface {nom}, changed state to {etat}"
    return f"%LINEPROTO-5-UPDOWN: Line protocol on Interface {nom}, changed state to {etat}"


def ospf(routeur):
    voisin = random.choice([v for r, v in VOISINS.items() if r != routeur])
    nom = random.choice(INTERFACES[:3])
    raison = random.choice([
        ("FULL", "DOWN", "Neighbor Down: Dead timer expired"),
        ("FULL", "DOWN", "Neighbor Down: Interface down or detached"),
        ("LOADING", "FULL", "Loading Done"),
    ])
    return (f"%OSPF-5-ADJCHG: Process 1, Nbr {voisin} on {nom} "
            f"from {raison[0]} to {raison[1]}, {raison[2]}")


def redemarrage(routeur):
    return random.choice([
        "%SYS-5-RESTART: System restarted --",
        f"%SNMP-5-COLDSTART: SNMP agent on host {routeur} is undergoing a cold start",
        "%SYS-6-BOOTTIME: Time taken to reboot after reload = "
        f"{random.randint(60, 240)} seconds",
    ])


def configuration(routeur):
    utilisateur = random.choice(UTILISATEURS[:2])
    ligne = random.choice(["console", f"vty{random.randint(0, 4)} ({ip_admin()})"])
    return f"%SYS-5-CONFIG_I: Configured from console by {utilisateur} on {ligne}"


def securite(routeur):
    utilisateur = random.choice(UTILISATEURS)
    if random.random() < 0.7:
        return (f"%SEC_LOGIN-4-LOGIN_FAILED: Login failed [user: {utilisateur}] "
                f"[Source: {ip_admin()}] [localport: 22] "
                "[Reason: Login Authentication Failed]")
    return (f"%SSH-5-SSH2_USERAUTH: User '{utilisateur}' authentication "
            f"for SSH2 Session from {ip_admin()} (tty = 0) using crypto cipher "
            "'aes128-ctr' Failed")


def ressources(routeur):
    if random.random() < 0.6:
        duree = random.randint(2001, 5000)
        return (f"%SYS-3-CPUHOG: Task is running for ({duree})msecs, more than "
                f"(2000)msecs, process = {random.choice(PROCESSUS)}")
    taille = random.choice([16384, 32768, 65536, 131072])
    return f"%SYS-2-MALLOCFAIL: Memory allocation of {taille} bytes failed"


FAMILLES = {
    "interface": interface,
    "ospf": ospf,
    "redemarrage": redemarrage,
    "configuration": configuration,
    "securite": securite,
    "ressources": ressources,
}


def creer_log(famille, date):
    routeur = random.choice(list(ROUTEURS))
    message = FAMILLES[famille](routeur)
    entete = message.split(":", 1)[0].lstrip("%")
    facility, severite, mnemonic = entete.split("-")
    return {
        "schema_version": "1.0",
        "source": "simulateur",
        "type": "syslog",
        "device_id": routeur,
        "device_type": "router",
        "timestamp": date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metrics": {},
        "meta": {"ip": ROUTEURS[routeur], "facility": facility, "mnemonic": mnemonic},
        "message": message,
        "severity": int(severite),
        "label": None,
        "scenario": famille,
    }


def generer(nombre, jours):
    fin = datetime.now(timezone.utc).replace(microsecond=0)
    debut = fin - timedelta(days=jours)
    familles = random.choices(list(REPARTITION), weights=REPARTITION.values(), k=nombre)
    dates = sorted(debut + timedelta(seconds=random.randint(0, jours * 86400))
                   for _ in range(nombre))
    return [creer_log(f, d) for f, d in zip(familles, dates)]


def ecrire(logs, chemin, fmt):
    with open(chemin, "w", newline="", encoding="utf-8") as fichier:
        if fmt == "jsonl":
            for log in logs:
                fichier.write(json.dumps(log, ensure_ascii=False) + "\n")
            return
        colonnes = ["timestamp", "device_id", "severity", "message", "scenario"]
        writer = csv.DictWriter(fichier, fieldnames=colonnes, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(logs)


def lire_arguments():
    parser = argparse.ArgumentParser(description="Génère des logs syslog Cisco pour M4")
    parser.add_argument("--nombre", type=int, default=600, help="nombre de logs")
    parser.add_argument("--jours", type=int, default=7, help="période couverte")
    parser.add_argument("--format", choices=["csv", "jsonl"], default="csv")
    parser.add_argument("--sortie", default="logs_m4.csv")
    parser.add_argument("--graine", type=int, default=42,
                        help="graine aléatoire, pour obtenir le même jeu à chaque fois")
    return parser.parse_args()


def main():
    args = lire_arguments()
    random.seed(args.graine)
    logs = generer(args.nombre, args.jours)
    ecrire(logs, args.sortie, args.format)

    print(f"{len(logs)} logs écrits dans {args.sortie}")
    for famille in REPARTITION:
        total = sum(1 for log in logs if log["scenario"] == famille)
        print(f"  {famille:<14} {total}")


if __name__ == "__main__":
    main()
