"""
Collecteur SNMP (partie 3).

Interroge les routeurs GNS3 en SNMP v2c à intervalle régulier, calcule les
métriques (CPU, mémoire, débit, erreurs, interfaces down) et les envoie dans
Kafka au format JSON commun (docs/format_json.md, section 4.2).
"""

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    get_cmd,
)
from pysnmp.proto.rfc1905 import EndOfMibView, NoSuchInstance, NoSuchObject

# MIB Cisco
OID_CPU_1MIN = "1.3.6.1.4.1.9.9.109.1.1.1.1.7.1"
OID_MEM_UTILISEE = "1.3.6.1.4.1.9.9.48.1.1.1.5.1"
OID_MEM_LIBRE = "1.3.6.1.4.1.9.9.48.1.1.1.6.1"

# IF-MIB, suivis de l'ifIndex de l'interface
OID_ETAT = "1.3.6.1.2.1.2.2.1.8"
OID_ERREURS_ENTREE = "1.3.6.1.2.1.2.2.1.14"
OID_OCTETS_ENTREE = "1.3.6.1.2.1.31.1.1.1.6"   # ifHCInOctets (64 bits)
OID_OCTETS_SORTIE = "1.3.6.1.2.1.31.1.1.1.10"  # ifHCOutOctets (64 bits)

# ifIndex rendus stables par « snmp-server ifindex persist »
INTERFACES = {1: "FastEthernet0/0", 2: "FastEthernet1/0", 3: "FastEthernet1/1"}

ROUTEURS_DEFAUT = "R1=192.168.10.1,R2=192.168.10.2,R3=192.168.10.3"
VALEURS_ABSENTES = (NoSuchObject, NoSuchInstance, EndOfMibView)


class Routeur:
    """Un routeur interrogé, avec ses compteurs du relevé précédent."""

    def __init__(self, nom, hote, port=161):
        self.nom = nom
        self.hote = hote
        self.port = port
        self.precedent = None  # (instant, octets_entree, octets_sortie, erreurs)

    def oids(self):
        liste = [OID_CPU_1MIN, OID_MEM_UTILISEE, OID_MEM_LIBRE]
        for index in INTERFACES:
            liste += [f"{OID_ETAT}.{index}", f"{OID_ERREURS_ENTREE}.{index}",
                      f"{OID_OCTETS_ENTREE}.{index}", f"{OID_OCTETS_SORTIE}.{index}"]
        return liste

    def calculer(self, valeurs, instant):
        """Transforme les valeurs SNMP brutes en métriques du format commun."""
        cpu = valeurs.get(OID_CPU_1MIN)
        utilisee = valeurs.get(OID_MEM_UTILISEE)
        libre = valeurs.get(OID_MEM_LIBRE)
        mem = None
        if utilisee is not None and libre:
            mem = round(utilisee / (utilisee + libre) * 100, 1)

        def total(oid):
            lus = [valeurs.get(f"{oid}.{i}") for i in INTERFACES]
            return None if None in lus else sum(lus)

        entree = total(OID_OCTETS_ENTREE)
        sortie = total(OID_OCTETS_SORTIE)
        erreurs = total(OID_ERREURS_ENTREE)
        etats = [valeurs.get(f"{OID_ETAT}.{i}") for i in INTERFACES]
        if_down = None if None in etats else sum(1 for e in etats if e != 1)

        # Débit et erreurs : différence avec le relevé précédent
        in_mbps = out_mbps = nouvelles_erreurs = None
        if self.precedent:
            t0, entree0, sortie0, erreurs0 = self.precedent
            duree = instant - t0
            in_mbps = debit(entree0, entree, duree)
            out_mbps = debit(sortie0, sortie, duree)
            if erreurs is not None and erreurs0 is not None and erreurs >= erreurs0:
                nouvelles_erreurs = erreurs - erreurs0
        self.precedent = (instant, entree, sortie, erreurs)

        return {
            "cpu_pct": float(cpu) if cpu is not None else None,
            "mem_pct": mem,
            "in_mbps": in_mbps,
            "out_mbps": out_mbps,
            "if_errors": nouvelles_erreurs,
            "if_down": if_down,
        }


def debit(avant, apres, duree):
    # Un compteur qui recule signifie un redémarrage du routeur : pas de débit calculable
    if avant is None or apres is None or duree <= 0 or apres < avant:
        return None
    return round((apres - avant) * 8 / duree / 1_000_000, 3)


async def interroger(moteur, communaute, routeur):
    """Lit tous les OID du routeur en une seule requête. Renvoie None s'il ne répond pas."""
    cible = await UdpTransportTarget.create((routeur.hote, routeur.port), timeout=2, retries=1)
    erreur, statut, _, resultats = await get_cmd(
        moteur, CommunityData(communaute, mpModel=1), cible, ContextData(),
        *[ObjectType(ObjectIdentity(oid)) for oid in routeur.oids()],
    )
    if erreur or statut:
        print(f"{routeur.nom} : {erreur or statut.prettyPrint()}", file=sys.stderr)
        return None
    return {
        str(oid): None if isinstance(valeur, VALEURS_ABSENTES) else int(valeur)
        for oid, valeur in resultats
    }


def message(routeur, metriques, horodatage):
    return {
        "schema_version": "1.0",
        "source": "gns3",
        "type": "snmp",
        "device_id": routeur.nom,
        "device_type": "router",
        "timestamp": horodatage,
        "metrics": metriques,
        "meta": {"ip": routeur.hote},
        "message": None,
        "severity": None,
        "label": None,
    }


def lire_routeurs(texte):
    """« R1=192.168.10.1,R2=192.168.10.2:1161 » -> liste de Routeur."""
    routeurs = []
    for element in texte.split(","):
        nom, adresse = element.strip().split("=")
        hote, _, port = adresse.partition(":")
        routeurs.append(Routeur(nom, hote, int(port or 161)))
    return routeurs


def creer_producteur(serveur):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    try:
        return KafkaProducer(
            bootstrap_servers=serveur,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
        )
    except NoBrokersAvailable:
        sys.exit(f"Kafka injoignable sur {serveur} : le conteneur kafka est-il démarré ?")


def lire_arguments():
    parser = argparse.ArgumentParser(description="Collecteur SNMP des routeurs GNS3")
    parser.add_argument("--routeurs", default=ROUTEURS_DEFAUT,
                        help="liste nom=ip[:port] séparée par des virgules")
    parser.add_argument("--communaute", default="projetRO", help="community SNMP v2c")
    parser.add_argument("--intervalle", type=float, default=10, help="secondes entre deux relevés")
    parser.add_argument("--cycles", type=int, default=0, help="nombre de relevés (0 : en continu)")
    parser.add_argument("--kafka", metavar="SERVEUR", help="adresse du broker Kafka, sinon affichage console")
    parser.add_argument("--topic", default="network-metrics", help="topic Kafka")
    return parser.parse_args()


async def collecter(args):
    routeurs = lire_routeurs(args.routeurs)
    producteur = creer_producteur(args.kafka) if args.kafka else None
    moteur = SnmpEngine()
    cycle = 0

    print("Routeurs :", ", ".join(f"{r.nom} ({r.hote})" for r in routeurs))
    try:
        while args.cycles == 0 or cycle < args.cycles:
            debut = time.monotonic()
            horodatage = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            reponses = await asyncio.gather(*[interroger(moteur, args.communaute, r) for r in routeurs])

            envoyes = 0
            for routeur, valeurs in zip(routeurs, reponses):
                if valeurs is None:
                    continue
                donnees = message(routeur, routeur.calculer(valeurs, debut), horodatage)
                if producteur:
                    producteur.send(args.topic, key=routeur.nom, value=donnees)
                else:
                    print(json.dumps(donnees, ensure_ascii=False))
                envoyes += 1

            if producteur:
                print(f"cycle {cycle} : {envoyes}/{len(routeurs)} routeurs envoyés dans {args.topic}")
            cycle += 1
            if args.cycles == 0 or cycle < args.cycles:
                await asyncio.sleep(max(0, args.intervalle - (time.monotonic() - debut)))
    finally:
        if producteur:
            producteur.flush()
            producteur.close()
        moteur.close_dispatcher()


def main():
    args = lire_arguments()
    try:
        asyncio.run(collecter(args))
    except KeyboardInterrupt:
        print("\narrêt demandé")


if __name__ == "__main__":
    main()
