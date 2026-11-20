"""Collecteur syslog : reçoit les logs des routeurs (UDP) et les publie dans Kafka."""

import json
import logging
import os
import re
import socket
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "network-syslog")
PORT = int(os.getenv("SYSLOG_PORT", "514"))

# Correspondance IP -> nom du routeur, ex : "192.168.10.1=R1,192.168.10.2=R2"
ROUTEURS = dict(
    p.split("=", 1) for p in os.getenv("ROUTEURS", "").split(",") if "=" in p
)

PRI = re.compile(r"^<(\d{1,3})>")
CISCO = re.compile(r"%(\w+)-(\d)-(\w+):\s*(.*)")

log = logging.getLogger("collecteur_syslog")


def analyser(brut, ip):
    """Transforme une ligne syslog brute en message au format JSON commun."""
    pri = PRI.match(brut)
    severite = int(pri.group(1)) % 8 if pri else None
    texte = brut[pri.end():].strip() if pri else brut.strip()

    meta = {"ip": ip}
    cisco = CISCO.search(texte)
    if cisco:
        meta["facility"] = cisco.group(1)
        meta["mnemonic"] = cisco.group(3)
        severite = int(cisco.group(2))
        message = cisco.group(0)
    else:
        message = texte

    return {
        "schema_version": "1.0",
        "source": "gns3",
        "type": "syslog",
        "device_id": ROUTEURS.get(ip, ip),
        "device_type": "router",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metrics": {},
        "meta": meta,
        "message": message,
        "severity": severite,
        "label": None,
    }


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", PORT))
    log.info("Écoute syslog sur UDP %d -> %s (%s)", PORT, KAFKA, TOPIC)

    try:
        while True:
            donnees, (ip, _) = sock.recvfrom(8192)
            msg = analyser(donnees.decode("utf-8", errors="replace"), ip)
            producer.send(TOPIC, key=msg["device_id"], value=msg)
            log.info("%s sev=%s %s", msg["device_id"], msg["severity"], msg["message"])
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        sock.close()


if __name__ == "__main__":
    main()
