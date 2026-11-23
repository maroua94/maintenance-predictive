"""
Génère un petit jeu de données au format CSV Backblaze (date, serial_number, model,
capacity_bytes, failure, smart_*_raw) pour tester les scripts Spark sans attendre
les vraies données dans HDFS.

  python tools/generate_sample_smart.py --disques 300 --jours 90 --sortie data/sample_smart.csv
"""
import argparse
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd

MODELES = [  # modèle, capacité (octets), probabilité de panne sur la période
    # ST12000NM0008 = modèle retenu par la partie 2 (SCRUM-14), majoritaire ici
    ("ST12000NM0008", 12_000_138_625_024, 0.06),
    ("ST12000NM0008", 12_000_138_625_024, 0.06),
    ("ST12000NM0008", 12_000_138_625_024, 0.06),
    ("ST4000DM000", 4_000_787_030_016, 0.10),
    ("TOSHIBA MG07ACA14TA", 14_000_519_643_136, 0.04),
]


def generer(nb_disques: int, nb_jours: int, debut: date, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    lignes = []
    for i in range(nb_disques):
        modele, capacite, p_panne = MODELES[i % len(MODELES)]
        serial = f"SN{i:05d}"
        age_h = int(rng.integers(2_000, 40_000))                # heures de fonctionnement au départ
        premier = int(rng.integers(0, nb_jours // 4))            # certains disques arrivent en cours de route
        jour_panne = int(rng.integers(premier + 10, nb_jours)) if rng.random() < p_panne else None
        dernier = jour_panne if jour_panne is not None else nb_jours - 1
        for j in range(premier, dernier + 1):
            rul = None if jour_panne is None else jour_panne - j
            degr = 0 if rul is None or rul > 30 else (30 - rul)   # dégradation sur les 30 derniers jours
            lignes.append({
                "date": (debut + timedelta(days=j)).isoformat(),
                "serial_number": serial,
                "model": modele,
                "capacity_bytes": capacite,
                "failure": int(rul == 0),
                "smart_5_raw": int(rng.poisson(0.2) + degr * rng.integers(0, 6)),
                "smart_9_raw": age_h + 24 * (j - premier),
                "smart_187_raw": int(rng.poisson(0.05) + degr * rng.integers(0, 2)),
                "smart_194_raw": int(rng.normal(31, 3)),
                "smart_197_raw": int(rng.poisson(0.1) + degr * rng.integers(0, 3)),
                "smart_198_raw": int(rng.poisson(0.1) + degr * rng.integers(0, 3)),
            })
    return pd.DataFrame(lignes)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--disques", type=int, default=300)
    p.add_argument("--jours", type=int, default=90)
    p.add_argument("--debut", default="2026-01-01")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sortie", default="data/sample_smart.csv")
    a = p.parse_args()

    df = generer(a.disques, a.jours, date.fromisoformat(a.debut), a.seed)
    os.makedirs(os.path.dirname(a.sortie) or ".", exist_ok=True)
    df.to_csv(a.sortie, index=False)
    print(f"{len(df)} lignes, {df.serial_number.nunique()} disques, {df.failure.sum()} pannes -> {a.sortie}")


if __name__ == "__main__":
    main()
