"""
Squelette du job Spark Structured Streaming (partie 1).

Lit en continu les 3 topics Kafka, décode le format JSON commun
(docs/format_json.md) et envoie les messages vers la console ou HDFS.
Les modèles M1, M3 et M4 viendront se brancher sur le DataFrame `messages`.

Lancement (depuis spark-master) :
    /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /opt/streaming/pipeline_streaming.py --sortie console
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, MapType, StringType,
                               StructField, StructType)

TOPICS = "servers-smart,network-metrics,network-syslog"

# Structure commune des messages Kafka (version 1.0)
SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("source", StringType()),
    StructField("type", StringType()),
    StructField("device_id", StringType()),
    StructField("device_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("metrics", MapType(StringType(), DoubleType())),
    StructField("meta", MapType(StringType(), StringType())),
    StructField("message", StringType()),
    StructField("severity", IntegerType()),
    StructField("label", IntegerType()),
])


def lire_kafka(spark, serveurs, depart):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", serveurs)
        .option("subscribe", TOPICS)
        .option("startingOffsets", depart)
        .option("failOnDataLoss", "false")
        .load()
    )


def decoder(brut):
    """Transforme la valeur Kafka (octets JSON) en colonnes du format commun."""
    messages = (
        brut.select(
            F.col("topic"),
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"),
        )
        .select("topic", "m.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
    )
    # Message illisible ou incomplet : on l'écarte
    return messages.filter(F.col("device_id").isNotNull() & F.col("event_time").isNotNull())


def ecrire(messages, sortie, hdfs, checkpoint):
    if sortie == "console":
        requete = (
            messages.select("topic", "device_id", "event_time", "metrics", "message", "severity")
            .writeStream.format("console")
            .option("truncate", "false")
            .option("numRows", 20)
        )
    else:
        requete = (
            messages.withColumn("jour", F.to_date("event_time"))
            .writeStream.format("parquet")
            .option("path", hdfs)
            .option("checkpointLocation", checkpoint)
            .partitionBy("type", "jour")
        )
    return requete.outputMode("append").trigger(processingTime="10 seconds").start()


def lire_arguments():
    parser = argparse.ArgumentParser(description="Lecture continue des topics Kafka")
    parser.add_argument("--kafka", default="kafka:29092", help="adresse interne de Kafka")
    parser.add_argument("--depart", choices=["latest", "earliest"], default="latest",
                        help="earliest pour relire tout l'historique des topics")
    parser.add_argument("--sortie", choices=["console", "hdfs"], default="console")
    parser.add_argument("--hdfs", default="hdfs://namenode:9000/data/stream/messages")
    parser.add_argument("--checkpoint", default="hdfs://namenode:9000/checkpoints/streaming")
    return parser.parse_args()


def main():
    args = lire_arguments()
    spark = (
        SparkSession.builder
        .appName("pipeline-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.hadoop.dfs.replication", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    messages = decoder(lire_kafka(spark, args.kafka, args.depart))
    requete = ecrire(messages, args.sortie, args.hdfs, args.checkpoint)
    requete.awaitTermination()


if __name__ == "__main__":
    main()
