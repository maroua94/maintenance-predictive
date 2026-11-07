# Format JSON commun des messages Kafka

> Version 1.0 — proposition à valider par l'équipe (SCRUM-20)

Toutes les sources (Backblaze, GNS3, simulateur) envoient leurs messages dans Kafka **avec la même structure**.
Comme ça, Spark peut tout lire de la même façon, et chaque partie sait exactement ce qu'elle reçoit.

---

## 1. Topics Kafka

| Topic | Contenu | Produit par | Lu par |
|---|---|---|---|
| `servers-smart` | Relevés SMART des disques (Backblaze rejoué) | Partie 2 | Spark → M1, M2 |
| `network-metrics` | Métriques SNMP des routeurs GNS3 + métriques du simulateur | Partie 3 | Spark → M3 |
| `network-syslog` | Logs syslog des routeurs GNS3 | Partie 3 (collecte) / Partie 1 | Spark → M4 |

- **Clé du message Kafka** = `device_id`. Tous les messages d'un même équipement vont dans la même partition, donc ils restent dans l'ordre. C'est indispensable pour les moyennes glissantes sur 7 jours.
- **Partitions :** 3 par topic (une par nœud du cluster).
- **Réplication :** 1 en local (S1), à passer à 2 sur le cluster 3 nœuds (S2).

---

## 2. Structure commune

```json
{
  "schema_version": "1.0",
  "source": "backblaze | gns3 | simulateur",
  "type": "smart | snmp | syslog | sim",
  "device_id": "identifiant unique de l'équipement",
  "device_type": "disk | router | switch | server",
  "timestamp": "2026-11-09T10:15:00Z",
  "metrics": { },
  "meta": { },
  "message": null,
  "severity": null,
  "label": null
}
```

| Champ | Type | Obligatoire | Description |
|---|---|---|---|
| `schema_version` | texte | oui | Version du format, `"1.0"` pour l'instant |
| `source` | texte | oui | D'où vient la donnée : `backblaze`, `gns3` ou `simulateur` |
| `type` | texte | oui | Nature de la mesure : `smart`, `snmp`, `syslog` ou `sim` |
| `device_id` | texte | oui | Identifiant unique : numéro de série du disque, `R1`, `SIM-042`… |
| `device_type` | texte | oui | `disk`, `router`, `switch` ou `server` |
| `timestamp` | texte | oui | Date et heure **ISO 8601 en UTC** (se termine par `Z`) |
| `metrics` | objet | oui | Les valeurs numériques mesurées (vide `{}` pour un syslog) |
| `meta` | objet | non | Infos fixes de l'équipement (modèle, capacité, IP…) |
| `message` | texte | syslog uniquement | Le texte du log, sinon `null` |
| `severity` | entier | syslog uniquement | Gravité syslog de 0 (urgence) à 7 (debug), sinon `null` |
| `label` | entier | Backblaze uniquement | Colonne `failure` (0 ou 1), sinon `null` |

---

## 3. Règles à respecter

1. **Noms en `snake_case`**, avec l'unité dans le nom quand il y en a une : `cpu_pct`, `temperature_c`, `throughput_mbps`.
2. **Toujours en UTC**, au format ISO 8601 : `2026-11-09T10:15:00Z`.
3. **Valeur manquante = `null`**. On ne supprime jamais la clé, pour que Spark garde le même schéma.
4. **Nombres en nombres**, pas en texte : `45.2` et non `"45.2"`.
5. **Un message = un équipement à un instant donné.**
6. **Encodage UTF-8.**
7. Les métriques communes au SNMP et au simulateur **portent le même nom** (`cpu_pct`, `mem_pct`…), pour que M3 les traite ensemble.

---

## 4. Exemples par source

### 4.1 Disque Backblaze — topic `servers-smart`

```json
{
  "schema_version": "1.0",
  "source": "backblaze",
  "type": "smart",
  "device_id": "ZA1FLE1P",
  "device_type": "disk",
  "timestamp": "2026-11-09T00:00:00Z",
  "metrics": {
    "smart_5_raw": 0,
    "smart_9_raw": 21543,
    "smart_187_raw": 0,
    "smart_194_raw": 31,
    "smart_197_raw": 8,
    "smart_198_raw": 8
  },
  "meta": {
    "model": "ST8000NM0055",
    "capacity_bytes": 8001563222016
  },
  "message": null,
  "severity": null,
  "label": 0
}
```

| Métrique | Signification |
|---|---|
| `smart_5_raw` | Secteurs réalloués |
| `smart_9_raw` | Heures de fonctionnement |
| `smart_187_raw` | Erreurs non corrigibles signalées |
| `smart_194_raw` | Température (°C) |
| `smart_197_raw` | Secteurs en attente de réallocation |
| `smart_198_raw` | Secteurs non corrigibles hors ligne |

### 4.2 Routeur GNS3 (SNMP) — topic `network-metrics`

```json
{
  "schema_version": "1.0",
  "source": "gns3",
  "type": "snmp",
  "device_id": "R1",
  "device_type": "router",
  "timestamp": "2026-11-09T10:15:00Z",
  "metrics": {
    "cpu_pct": 12.0,
    "mem_pct": 38.5,
    "in_mbps": 4.2,
    "out_mbps": 3.9,
    "if_errors": 0,
    "if_down": 0
  },
  "meta": {
    "ip": "192.168.10.1",
    "interface": "GigabitEthernet0/0"
  },
  "message": null,
  "severity": null,
  "label": null
}
```

### 4.3 Équipement simulé — topic `network-metrics`

```json
{
  "schema_version": "1.0",
  "source": "simulateur",
  "type": "sim",
  "device_id": "SIM-042",
  "device_type": "switch",
  "timestamp": "2026-11-09T10:15:00Z",
  "metrics": {
    "cpu_pct": 55.1,
    "mem_pct": 62.3,
    "temperature_c": 48.0,
    "throughput_mbps": 210.5,
    "crc_errors": 2,
    "packet_loss_pct": 0.3
  },
  "meta": {
    "scenario": "normal"
  },
  "message": null,
  "severity": null,
  "label": null
}
```

> `meta.scenario` vaut `normal`, `surchauffe` ou `fuite_memoire`. Ce champ sert uniquement à évaluer M3 (taux de pannes provoquées détectées). **Le modèle ne doit jamais l'utiliser comme entrée.**

### 4.4 Log syslog — topic `network-syslog`

```json
{
  "schema_version": "1.0",
  "source": "gns3",
  "type": "syslog",
  "device_id": "R2",
  "device_type": "router",
  "timestamp": "2026-11-09T10:16:42Z",
  "metrics": {},
  "meta": {
    "facility": "LINK"
  },
  "message": "%LINK-3-UPDOWN: Interface GigabitEthernet0/1, changed state to down",
  "severity": 3,
  "label": null
}
```

---

## 5. Points à valider en réunion

- [ ] Les 3 noms de topics
- [ ] La liste des champs communs
- [ ] Les noms des métriques SNMP (partie 3)
- [ ] Les noms des métriques du simulateur (partie 3)
- [ ] Les attributs SMART retenus (partie 2)
- [ ] Ce dont la partie 4 a besoin pour InfluxDB / Grafana

Une fois validé : plus aucun changement sans prévenir toute l'équipe. Si on doit modifier le format, on passe `schema_version` à `1.1`.
