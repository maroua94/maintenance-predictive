# Partie 1 — Infrastructure & DevOps

Cluster Big Data conteneurisé de la plateforme de maintenance prédictive : **Kafka**, **HDFS** et **Spark**, lancés avec une seule commande.

Responsable : Maroua Ettouri

---

## Architecture

| Service | Conteneur(s) | Image | Rôle |
|---|---|---|---|
| Kafka (KRaft) | `kafka` | `apache/kafka:3.7.0` | Transport des données en temps réel |
| Init Kafka | `kafka-init` | `apache/kafka:3.7.0` | Crée les topics au démarrage, puis s'arrête |
| HDFS NameNode | `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | Gère l'arborescence et la position des blocs |
| HDFS DataNodes | `datanode1`, `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | Stockent les blocs (réplication 2) |
| Spark Master | `spark-master` | `apache/spark:3.5.1` | Distribue les traitements |
| Spark Workers | `spark-worker1`, `spark-worker2` | `apache/spark:3.5.1` | Exécutent les traitements (2 cœurs, 2 Go chacun) |

Les versions sont figées volontairement : tout le monde utilise exactement le même cluster.

---

## Prérequis

- Docker Desktop avec l'intégration WSL 2 activée (sous Windows)
- Docker Compose v2 (`docker compose version`)
- Environ 6 Go de RAM disponibles pour Docker

---

## Lancer le cluster

```bash
cd partie1-infra
docker compose up -d
docker compose ps
```

Résultat attendu : tous les conteneurs sont `Up`, et `kafka-init` est `Exited (0)` (normal, il a fini son travail).

Arrêter le cluster :

```bash
docker compose down          # arrête les conteneurs, garde les données HDFS
docker compose down -v       # arrête et SUPPRIME les données HDFS
```

---

## Ports et adresses

| Service | Depuis ta machine | Depuis un autre conteneur |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` |
| HDFS (interface web) | http://localhost:9870 | — |
| HDFS (accès fichiers) | `localhost:9000` | `hdfs://namenode:9000` |
| Spark (interface web) | http://localhost:8080 | — |
| Spark Master | `localhost:7077` | `spark://spark-master:7077` |

> Les scripts Python lancés sur ta machine utilisent `localhost:9092`.
> Les jobs Spark (dans le cluster) utilisent `kafka:29092`.

---

## Topics Kafka

Créés automatiquement au démarrage (3 partitions chacun) :

| Topic | Contenu |
|---|---|
| `servers-smart` | Relevés SMART des disques (Backblaze) |
| `network-metrics` | Métriques SNMP (GNS3) et simulateur |
| `network-syslog` | Logs syslog des routeurs |

Le format des messages est décrit dans [`docs/format_json.md`](../docs/format_json.md).

---

## Tester le cluster

### 1. Kafka : envoyer et recevoir un message

Terminal 1 (consommateur) :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic network-metrics --from-beginning
```

Terminal 2 (producteur), puis coller un message JSON et appuyer sur Entrée :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic network-metrics
```

Résultat attendu : le message apparaît dans le terminal 1.

### 2. HDFS : écrire et lire un fichier

```bash
docker exec -it namenode bash -c "echo 'test hdfs' > /tmp/test.txt && \
  hdfs dfs -mkdir -p /data/test && \
  hdfs dfs -put -f /tmp/test.txt /data/test/ && \
  hdfs dfs -ls /data/test && \
  hdfs dfs -cat /data/test/test.txt"
```

Résultat attendu : le fichier est listé avec une réplication de `2`, puis son contenu s'affiche.
Interface web : http://localhost:9870 → onglet **Datanodes** → 2 nœuds en service.

### 3. Spark : calcul distribué

```bash
docker exec -it spark-master /opt/spark/bin/run-example \
  --master spark://spark-master:7077 SparkPi 10
```

Résultat attendu : `Pi is roughly 3.14...`
Interface web : http://localhost:8080 → **Alive Workers: 2**.

### 4. Spark + HDFS : lire un fichier HDFS depuis Spark

```bash
docker exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077
```

Puis dans `scala>` :

```scala
spark.read.textFile("hdfs://namenode:9000/data/test/test.txt").show(false)
:quit
```

Résultat attendu : un tableau contenant le texte du fichier.

---

## Dépannage

| Problème | Solution |
|---|---|
| `docker: command not found` dans WSL | Docker Desktop → Settings → Resources → WSL integration → activer la distribution |
| `permission denied ... docker.sock` | `sudo usermod -aG docker $USER` puis `wsl --shutdown` et rouvrir le terminal |
| Un conteneur s'arrête avec `Exited (137)` | Manque de mémoire : augmenter la RAM allouée à Docker |
| Les DataNodes n'apparaissent pas sur :9870 | Attendre 1 minute, puis `docker logs datanode1` |

---

## Évolutions prévues

- **S2** : passage sur 3 nœuds, persistance Kafka, CI/CD GitHub Actions, logs syslog vers Kafka
- **S3** : squelette du job Spark Streaming, modèle M4 (analyse des logs)
- **S4** : intégration de tous les modèles dans Spark Streaming
