"""Tests de la partie 4 (RUL, format commun, écriture InfluxDB) — lancer avec : pytest partie4-restitution/tests"""

import os
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
from commun import normaliser  # noqa: E402
from failure_stats import ecrire_influx, ligne  # noqa: E402
from rul_label import ajouter_rul  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests-partie4").getOrCreate()
    yield s
    s.stop()


def _jours(device, n, jour_panne=None):
    d0 = date(2026, 1, 1)
    return [(d0 + timedelta(days=i), device, int(i == jour_panne)) for i in range(n)]


def test_disque_en_panne(spark):
    df = spark.createDataFrame(_jours("A", 5, jour_panne=4), ["date", "device_id", "label"])
    rows = ajouter_rul(df, plafond=3).orderBy("date").collect()
    assert [r.rul_jours for r in rows] == [4, 3, 2, 1, 0]
    assert [r.rul_plafonne for r in rows] == [3, 3, 2, 1, 0]
    assert all(r.date_panne == date(2026, 1, 5) for r in rows)


def test_disque_sain_censure(spark):
    df = spark.createDataFrame(_jours("B", 60), ["date", "device_id", "label"])
    rows = ajouter_rul(df, plafond=30).orderBy("date").collect()
    assert all(r.rul_jours is None for r in rows)
    # encore observé >= 30 jours -> 30 ; sinon inconnu (null)
    assert [r.rul_plafonne for r in rows[:30]] == [30] * 30
    assert all(r.rul_plafonne is None for r in rows[30:])


def test_disques_independants(spark):
    df = spark.createDataFrame(_jours("A", 5, jour_panne=4) + _jours("B", 5), ["date", "device_id", "label"])
    b = ajouter_rul(df, plafond=30).where("device_id = 'B'").collect()
    assert all(r.date_panne is None for r in b)


def test_normaliser_csv_backblaze(spark):
    df = spark.createDataFrame([("2026-01-01", "ZA1", "ST12000NM0008", 0, 3)],
                               ["date", "serial_number", "model", "failure", "smart_5_raw"])
    r = normaliser(df).first()
    assert (r.device_id, r.label, r.date, r.smart_5_raw) == ("ZA1", 0, date(2026, 1, 1), 3)


def test_normaliser_format_kafka(spark):
    message = ('{"device_id": "ZA1", "timestamp": "2026-01-09T00:00:00Z", "label": 1,'
               ' "metrics": {"smart_5_raw": 8}, "meta": {"model": "ST12000NM0008"}}')
    df = spark.read.json(spark.sparkContext.parallelize([message]))
    r = normaliser(df).first()
    assert (r.device_id, r.label, r.date, r.smart_5_raw, r.model) == \
        ("ZA1", 1, date(2026, 1, 9), 8, "ST12000NM0008")


def test_ligne_protocol():
    t = datetime(2026, 1, 31, tzinfo=timezone.utc)
    assert ligne("afr_modele", {"modele": "WDC WUH721414", "fabricant": "Western Digital"},
                 {"nb_pannes": 3, "afr_pct": 1.5, "vide": None}, t) == \
        r"afr_modele,fabricant=Western\ Digital,modele=WDC\ WUH721414 nb_pannes=3i,afr_pct=1.5 1769817600"
    assert ligne("kpi", {}, {"x": None}, t) is None


def test_ecriture_influx():
    recu = {}

    class Faux(BaseHTTPRequestHandler):
        def do_POST(self):
            recu["chemin"] = self.path
            recu["token"] = self.headers["Authorization"]
            recu["corps"] = self.rfile.read(int(self.headers["Content-Length"])).decode()
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    serveur = HTTPServer(("127.0.0.1", 0), Faux)
    threading.Thread(target=serveur.handle_request, daemon=True).start()
    ecrire_influx(["kpi nb_pannes=3i 1769817600"], f"http://127.0.0.1:{serveur.server_port}",
                  "tok", "ensa", "maintenance")
    serveur.server_close()
    assert recu["chemin"] == "/api/v2/write?org=ensa&bucket=maintenance&precision=s"
    assert recu["token"] == "Token tok"
    assert recu["corps"] == "kpi nb_pannes=3i 1769817600"
