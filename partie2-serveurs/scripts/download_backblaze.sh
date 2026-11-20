#!/usr/bin/env bash
# Télécharge un trimestre Backblaze et extrait un mois de CSV.
# Usage   : ./download_backblaze.sh <trimestre> <année> <mois>
# Exemple : ./download_backblaze.sh Q1 2026 01
# Les données sont rangées dans $DATA_DIR (par défaut ~/data/backblaze), hors du dépôt Git.
set -euo pipefail

QUARTER="${1:-Q1}"
YEAR="${2:-2026}"
MONTH="${3:-01}"
DATA_DIR="${DATA_DIR:-$HOME/data/backblaze}"

ZIP="data_${QUARTER}_${YEAR}.zip"
URL="https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/${ZIP}"

mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

if [ ! -f "$ZIP" ]; then
  echo ">> Téléchargement de $ZIP ..."
  wget -c "$URL"
else
  echo ">> $ZIP déjà présent, téléchargement ignoré."
fi

echo ">> Extraction des fichiers de ${YEAR}-${MONTH} ..."
unzip -n -q "$ZIP" "*${YEAR}-${MONTH}-*.csv" -x "__MACOSX/*" -d "$DATA_DIR"

COUNT=$(find "$DATA_DIR" -name "${YEAR}-${MONTH}-*.csv" | wc -l)
echo ">> $COUNT fichiers CSV disponibles dans $DATA_DIR"
