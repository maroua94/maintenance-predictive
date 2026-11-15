"""
Simulateur d'équipements réseau (partie 3).

Génère des mesures pour un parc d'équipements au format JSON commun
(voir docs/format_json.md). Une partie des équipements développe une panne
progressive : surchauffe ou fuite mémoire.
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

TYPES_EQUIPEMENT = ("switch", "router", "server")
SCENARIOS_PANNE = ("surchauffe", "fuite_memoire")
TAUX_PANNE = 0.10

# Seuils à partir desquels une dérive commence à dégrader les autres métriques
SEUIL_TEMPERATURE = 65.0
SEUIL_MEMOIRE = 85.0


def borner(valeur, mini, maxi):
    return max(mini, min(maxi, valeur))


class Equipement:
    """Équipement simulé avec ses valeurs de référence et son scénario."""

    def __init__(self, numero, nb_cycles):
        self.device_id = f"SIM-{numero:03d}"
        self.device_type = random.choice(TYPES_EQUIPEMENT)

        self.cpu_ref = random.uniform(10, 40)
        self.mem_ref = random.uniform(30, 55)
        self.temp_ref = random.uniform(35, 45)
        self.debit_ref = random.uniform(50, 300)

        if random.random() < TAUX_PANNE:
            self.scenario = random.choice(SCENARIOS_PANNE)
            self.debut_panne = random.randint(nb_cycles // 4, nb_cycles // 2)
        else:
            self.scenario = "normal"
            self.debut_panne = None

        self.mem_perdue = 0.0

    def en_panne(self, cycle):
        return self.debut_panne is not None and cycle >= self.debut_panne

    def _mesures_normales(self):
        return {
            "cpu": self.cpu_ref + random.gauss(0, 3),
            "mem": self.mem_ref + random.gauss(0, 1),
            "temp": self.temp_ref + random.gauss(0, 0.5),
            "in": self.debit_ref + random.gauss(0, 15),
            "out": self.debit_ref * 0.9 + random.gauss(0, 15),
            "erreurs": random.choices([0, 1, 2], weights=[90, 8, 2])[0],
            "perte": abs(random.gauss(0, 0.1)),
        }

    def _appliquer_surchauffe(self, m, duree):
        m["temp"] += 0.4 * duree
        depassement = m["temp"] - SEUIL_TEMPERATURE
        if depassement > 0:
            m["cpu"] += depassement * 1.5
            m["perte"] += depassement * 0.15
            m["erreurs"] += int(depassement / 3)

    def _appliquer_fuite_memoire(self, m):
        self.mem_perdue += random.uniform(0.2, 0.5)
        m["mem"] += self.mem_perdue
        depassement = m["mem"] - SEUIL_MEMOIRE
        if depassement > 0:
            m["cpu"] += depassement * 2
            m["perte"] += depassement * 0.1

    def mesurer(self, cycle, horodatage):
        m = self._mesures_normales()
        panne = self.en_panne(cycle)

        if panne and self.scenario == "surchauffe":
            self._appliquer_surchauffe(m, cycle - self.debut_panne)
        elif panne and self.scenario == "fuite_memoire":
            self._appliquer_fuite_memoire(m)

        return {
            "schema_version": "1.0",
            "source": "simulateur",
            "type": "sim",
            "device_id": self.device_id,
            "device_type": self.device_type,
            "timestamp": horodatage,
            "metrics": {
                "cpu_pct": round(borner(m["cpu"], 0, 100), 1),
                "mem_pct": round(borner(m["mem"], 0, 100), 1),
                "temperature_c": round(borner(m["temp"], 20, 110), 1),
                "in_mbps": round(max(0.0, m["in"]), 1),
                "out_mbps": round(max(0.0, m["out"]), 1),
                "if_errors": m["erreurs"],
                "packet_loss_pct": round(borner(m["perte"], 0, 100), 2),
            },
            # Utilisé uniquement pour évaluer M3, jamais comme entrée du modèle
            "meta": {"scenario": self.scenario if panne else "normal"},
            "message": None,
            "severity": None,
            "label": None,
        }


def lire_arguments():
    parser = argparse.ArgumentParser(description="Simulateur d'équipements réseau")
    parser.add_argument("--equipements", type=int, default=200, help="nombre d'équipements")
    parser.add_argument("--cycles", type=int, default=10, help="nombre de relevés")
    parser.add_argument("--intervalle", type=float, default=10,
                        help="secondes entre deux relevés (0 : génération immédiate)")
    parser.add_argument("--sortie", help="fichier JSON Lines de sortie (sinon affichage)")
    parser.add_argument("--graine", type=int, default=42, help="graine aléatoire")
    return parser.parse_args()


def main():
    args = lire_arguments()
    random.seed(args.graine)

    parc = [Equipement(i, args.cycles) for i in range(1, args.equipements + 1)]
    pannes = [e for e in parc if e.scenario != "normal"]
    print(f"{len(parc)} équipements, {len(pannes)} pannes programmées")
    for e in pannes:
        print(f"  {e.device_id:<8} {e.device_type:<7} {e.scenario:<14} cycle {e.debut_panne}")

    debut = datetime.now(timezone.utc).replace(microsecond=0)
    pas = args.intervalle if args.intervalle > 0 else 10
    fichier = open(args.sortie, "w", encoding="utf-8") if args.sortie else None

    try:
        for cycle in range(args.cycles):
            horodatage = (debut + timedelta(seconds=cycle * pas)).strftime("%Y-%m-%dT%H:%M:%SZ")
            for e in parc:
                ligne = json.dumps(e.mesurer(cycle, horodatage), ensure_ascii=False)
                print(ligne, file=fichier)  # file=None : affichage console
            if args.intervalle > 0 and cycle < args.cycles - 1:
                time.sleep(args.intervalle)
    finally:
        if fichier:
            fichier.close()

    if args.sortie:
        print(f"{args.cycles * len(parc)} messages écrits dans {args.sortie}")


if __name__ == "__main__":
    main()
