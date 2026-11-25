# Partie 2 — Serveurs & Big Data (Imane)

Données SMART des disques (Backblaze) : récupération, nettoyage avec Spark, étiquette « panne dans 7 jours » pour M1.

## Données

- Modèle de disque retenu : **ST12000NM0008** (Seagate, 12 To) — voir `notebooks/01_exploration_smart.ipynb`
- Attributs SMART : 5, 9, 187, 194, 197, 198 (valeurs `_raw`)
- Période d'entraînement : octobre 2025 → mars 2026

## Utilisation

Télécharger un mois (données rangées dans `~/data/backblaze`, hors du dépôt) :

    ./partie2-serveurs/scripts/download_backblaze.sh Q4 2025 10

Nettoyer et étiqueter, en local :

    spark-submit --driver-memory 6g partie2-serveurs/traitement/nettoyage_smart.py \
      --entree "$HOME/data/backblaze/data_Q*/20*.csv" --format csv \
      --sortie "$HOME/data/backblaze/clean"

Sur le cluster, les valeurs par défaut lisent `hdfs://namenode:9000/data/smart/raw` et écrivent dans `hdfs://namenode:9000/data/smart/clean`.

## Sortie (`/data/smart/clean`)

`device_id`, `date`, `model`, `label` (1 le jour de la panne), `label_7j` (1 de J-6 à J), `smart_*_raw`.

Résultat sur 6 mois : 3 269 213 lignes, 18 771 disques, 241 pannes, 1 360 lignes `label_7j = 1`.

Vérification : les 241 disques en panne ont une étiquette ; 63 % ont leurs 7 jours complets (les autres ont des jours de relevé manquants avant la panne).