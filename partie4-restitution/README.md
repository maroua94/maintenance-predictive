# Partie 4 — Restitution & Dashboard (Hamza)

Calcul des étiquettes et des statistiques de pannes avec **Spark**, stockage dans **InfluxDB**, affichage dans **Grafana**.

Responsable : Hamza El Ouskini

---

## Architecture

~~~text
 Kafka servers-smart → HDFS /data/smart/clean (partie 2)
                              │
                              ▼
        spark/rul_label.py (SCRUM-40)      → HDFS /data/smart/rul
                              │
                              ▼
        spark/failure_stats.py (SCRUM-39)  → InfluxDB, bucket "maintenance"
                                                   │
                                                   ▼
                                Grafana : dashboard « Maintenance prédictive » (SCRUM-19)
~~~

| Élément | Fichier | Ticket |
|---|---|---|
| InfluxDB 2.7.12 + Grafana 13.3.0 | `docker-compose.yml` | SCRUM-18 |
| Datasource et dossier de dashboards créés au démarrage | `grafana/provisioning/` | SCRUM-18 |
| Dashboard Grafana (11 panneaux) | `grafana/dashboards/maintenance-predictive.json` | SCRUM-19 |
| Branchement sur le réseau du cluster | `docker-compose.cluster.yml` | SCRUM-38 |
| Étiquette RUL (jours restants avant la panne) | `spark/rul_label.py` | SCRUM-40 |
| Statistiques de pannes en Spark SQL → InfluxDB | `spark/failure_stats.py` | SCRUM-39 |
| Lecture et format commun, configuration Spark | `spark/commun.py` | — |
| Jeu de données de test au format Backblaze | `tools/generate_sample_smart.py` | — |
| Tests (RUL, format commun, écriture InfluxDB) | `tests/test_rul.py` | — |

Les versions sont figées, comme pour le cluster. On utilise InfluxDB 2 parce que, depuis mai 2026, l'étiquette `influxdb:latest` désigne InfluxDB 3, dont l'API est différente.

---

## Lancer InfluxDB et Grafana

**Seuls, en local (SCRUM-18)**

```bash
cd partie4-restitution
cp .env.example .env        # facultatif : sinon les identifiants par défaut du compose sont utilisés
docker compose up -d
docker compose ps           # influxdb doit être "healthy"
```

**Avec le cluster de la partie 1 (SCRUM-38)**

```bash
cd partie1-infra && docker compose up -d && cd ..
cd partie4-restitution
docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d
docker exec spark-master python3 -c "import urllib.request; print(urllib.request.urlopen('http://influxdb:8086/health').read())"
```

Résultat attendu : `"status":"pass"`. Spark voit alors InfluxDB.

---

## Ports et adresses

| Service | Depuis ta machine | Depuis un conteneur du cluster |
|---|---|---|
| InfluxDB | http://localhost:8086 | `http://influxdb:8086` |
| Grafana | http://localhost:3000 | `http://grafana:3000` |

Identifiants par défaut (à changer dans `.env`) :

| Service | Valeur |
|---|---|
| InfluxDB | utilisateur `admin`, org `ensa`, bucket `maintenance`, token `maintenance-token-dev` |
| Grafana | utilisateur `admin`, mot de passe `admin` |

---

## Lancer les jobs Spark dans le cluster

```bash
docker cp partie4-restitution/spark spark-master:/opt/spark/work-dir/partie4

# SCRUM-40 : étiquette RUL
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/partie4/rul_label.py

# SCRUM-39 : statistiques -> InfluxDB (+ copie CSV dans HDFS)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/partie4/failure_stats.py \
  --influx-token maintenance-token-dev --sortie-stats hdfs://namenode:9000/data/smart/stats
```

| Option | Défaut | Rôle |
|---|---|---|
| `--entree` (RUL) | `hdfs://namenode:9000/data/smart/clean` | relevés SMART nettoyés par la partie 2 |
| `--sortie` (RUL) | `hdfs://namenode:9000/data/smart/rul` | Parquet partitionné par date |
| `--plafond` | `30` | RUL maximum, à garder bien plus court que la période couverte |
| `--entree` (stats) | `hdfs://namenode:9000/data/smart/rul` | sortie du job RUL |
| `--format` | `parquet` | `csv` (Backblaze brut) ou `json` (messages Kafka) aussi acceptés |
| `--no-influx` | — | calcule et affiche sans écrire dans InfluxDB |

Les jobs n'ont besoin d'aucune bibliothèque en plus : l'écriture dans InfluxDB passe par son API HTTP.

---

## Données attendues en entrée

Les jobs lisent le format commun du projet ([`docs/format_json.md`](../docs/format_json.md)), sous trois formes possibles : le Parquet aplati de la partie 2, les messages Kafka (`metrics.*` et `meta.*`) ou les CSV Backblaze bruts.

| Colonne | Format commun | CSV Backblaze |
|---|---|---|
| identifiant du disque | `device_id` | `serial_number` |
| jour | `date`, `event_time` ou `timestamp` | `date` |
| panne ce jour (0/1) | `label` | `failure` |
| modèle | `model` ou `meta.model` | `model` |
| attributs SMART | `smart_5_raw`, `smart_9_raw`, `smart_187_raw`, `smart_194_raw`, `smart_197_raw`, `smart_198_raw` | idem |

Les attributs sont ceux qu'a retenus la partie 2 (SCRUM-14). Si l'un d'eux manque, il est simplement ignoré.

---

## Étiquette RUL (SCRUM-40)

| Colonne ajoutée | Signification |
|---|---|
| `date_panne` | date de la prochaine panne du disque, null s'il ne tombe pas en panne |
| `rul_jours` | jours restants avant la panne, 0 le jour de la panne |
| `rul_plafonne` | RUL limité à `--plafond`. Un disque sain encore observé au moins `--plafond` jours reçoit cette valeur ; sinon null (ligne censurée) |

L'étiquette « panne dans 7 jours » de la partie 2 (SCRUM-34) correspond à `rul_jours <= 7`.

---

## Measurements InfluxDB (SCRUM-39)

| Measurement | Tags | Champs |
|---|---|---|
| `kpi` | — | `nb_disques`, `nb_pannes`, `jours_disque`, `afr_pct`, `age_moyen_panne_ans` |
| `pannes_jour` | — | `nb_disques`, `nb_pannes`, `taux_pannes_pct` (un point par jour) |
| `afr_modele` | `modele`, `fabricant` | `nb_disques`, `jours_disque`, `nb_pannes`, `afr_pct` |
| `smart_reference` | `attribut`, `groupe` (sain / panne) | `valeur` (moyenne) |
| `smart_non_nul` | `attribut`, `groupe` | `valeur` (% de disques avec compteur > 0 au dernier relevé) |
| `smart_avant_panne` | `rul_jours` | `smart_5_raw`, `smart_187_raw`, `smart_197_raw`, `smart_198_raw` |

- **AFR** (taux de panne annualisé, formule Backblaze) = pannes ÷ (jours-disque ÷ 365) × 100.
- Les statistiques globales sont datées du dernier jour des données. Relancer le job sur les mêmes données écrase les points au lieu de les dupliquer.

---

## Tester sans cluster

```bash
pip install -r partie4-restitution/spark/requirements.txt
cd partie4-restitution
pytest tests/
python tools/generate_sample_smart.py --sortie data/sample_smart.csv
cd spark
spark-submit rul_label.py --entree ../data/sample_smart.csv --format csv --sortie ../out/smart_rul
spark-submit failure_stats.py --entree ../out/smart_rul --no-influx --min-jours-disque 500
```

Avec InfluxDB lancé en local, il suffit de remplacer `--no-influx` par `--influx-url http://localhost:8086 --influx-token maintenance-token-dev`, puis d'ouvrir Grafana.

---

## Dashboard (SCRUM-19)

Grafana → Dashboards → **Maintenance prédictive** → *Maintenance prédictive — Disques (SMART)*.

| Rangée | Panneaux |
|---|---|
| Indicateurs | disques suivis · pannes · AFR global · âge moyen à la panne |
| Évolution | pannes par jour · disques suivis par jour |
| Comparaison | % de disques avec compteur d'erreurs > 0 (sains / en panne) · AFR par modèle |
| Avant la panne | attributs SMART de J-30 à J-0 · moyennes sains / en panne |

La plage de temps par défaut est de 2 ans. Il faut l'ajuster à la période des données Backblaze rejouées.

**Prévu ensuite** : une rangée « disques à risque » (S3, après le modèle M1/M2) et une rangée réseau SNMP / syslog (données des parties 1 et 3).

---

## Dépannage

| Problème | Solution |
|---|---|
| `network maintenance-predictive_default not found` | Le cluster de la partie 1 n'est pas lancé |
| Grafana : « No data » | Vérifier la plage de temps, puis que le job SCRUM-39 a bien écrit (`--line-protocol points.txt` pour voir les points) |
| `InfluxDB a refusé l'écriture (401)` | Le token passé au job n'est pas celui du `.env` |
| Changement du token après le premier lancement | InfluxDB garde l'ancien : `docker compose down -v` puis relancer |
