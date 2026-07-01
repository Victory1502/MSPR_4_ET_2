"""
Job Spark Structured Streaming - transformation, qualite et historisation VelibData.

Principe retenu : trigger `availableNow=True` -> le job consomme uniquement les messages
Kafka disponibles au moment de son lancement puis s'arrete (pas de streaming continu).
Cela respecte la contrainte du sujet ("ne pas pousser l'aspect temps reel trop loin") tout
en gardant, via le checkpoint Spark, la position exacte de lecture entre deux executions
(le job peut etre relance toutes les X minutes par un ordonnanceur, cf. plan_maintenance.md).

Couches produites :
  - bronze (MinIO)   : copie brute immuable de chaque message Kafka, pour rejouabilite/audit.
  - silver (Postgres) : donnees nettoyees, dedupliquees, normalisees, requetables par les
                        Data Analysts / Data Scientists.
  - pipeline_alertes  : toute anomalie de qualite ou de fraicheur detectee pendant le batch.

Lancement (depuis le conteneur spark-master, cf. spark_jobs/run_job.sh) :
  spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    /opt/spark-apps/spark_jobs/velib_batch_processing.py
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                                 StructField, StructType)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("velib_batch_processing")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_STATIONS = os.environ.get("KAFKA_TOPIC_STATIONS", "velib.stations.raw")
TOPIC_DISPONIBILITE = os.environ.get("KAFKA_TOPIC_DISPONIBILITE", "velib.disponibilite.raw")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.environ["MINIO_ROOT_USER"]
MINIO_ROOT_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "velib-data")

POSTGRES_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)

# Regles de qualite (cf. cahier des charges fonctionnel, section 2.3)
REJECT_RATE_ALERT_THRESHOLD = 0.02
FRESHNESS_ALERT_THRESHOLD_MINUTES = 10

STATION_RESULT_SCHEMA = StructType([
    StructField("stationcode", StringType()),
    StructField("name", StringType()),
    StructField("capacity", LongType()),
    StructField("coordonnees_geo", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType()),
    ])),
])

DISPONIBILITE_RESULT_SCHEMA = StructType([
    StructField("stationcode", StringType()),
    StructField("name", StringType()),
    StructField("capacity", LongType()),
    StructField("numdocksavailable", LongType()),
    StructField("numbikesavailable", LongType()),
    StructField("ebike", LongType()),
    StructField("is_renting", StringType()),
    StructField("is_returning", StringType()),
    StructField("duedate", StringType()),
    StructField("coordonnees_geo", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType()),
    ])),
    StructField("nom_arrondissement_communes", StringType()),
])


def envelope_schema(result_schema: StructType) -> StructType:
    return StructType([
        StructField("source", StringType()),
        StructField("fetched_at", StringType()),
        StructField("batch_id", StringType()),
        StructField("payload", StructType([
            StructField("total_count", LongType()),
            StructField("results", ArrayType(result_schema)),
        ])),
    ])


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3_client) -> None:
    # Les deux requetes streaming (stations/disponibilite) tournent sur des threads
    # concurrents et peuvent toutes deux constater l'absence du bucket avant que l'une
    # ne l'ait cree : on ignore donc l'erreur "bucket deja existant" de la course gagnee
    # par l'autre thread.
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        try:
            s3_client.create_bucket(Bucket=MINIO_BUCKET)
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass


def archive_bronze(raw_rows, source: str) -> str | None:
    """Historise chaque message Kafka brut dans MinIO (couche bronze, immuable, rejouable)."""
    if not raw_rows:
        return None
    s3_client = get_s3_client()
    ensure_bucket(s3_client)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    last_batch_id = None
    for row in raw_rows:
        envelope = json.loads(row.value)
        last_batch_id = envelope.get("batch_id", "unknown")
        key = f"bronze/{source}/dt={today}/{last_batch_id}.json"
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=row.value, ContentType="application/json")
    logger.info("Bronze MinIO : %d message(s) archive(s) pour '%s'", len(raw_rows), source)
    return last_batch_id


def log_alert(alerte_type: str, severity: str, description: str, metric_value: float = None, batch_id: str = None) -> None:
    logger.warning("ALERTE [%s/%s] %s", severity, alerte_type, description)
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_alertes (alerte_type, severity, description, metric_value, batch_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (alerte_type, severity, description, metric_value, batch_id),
            )


def process_stations_batch(batch_df, epoch_id) -> None:
    if batch_df.rdd.isEmpty():
        return

    raw_rows = batch_df.select("value").collect()
    batch_id = archive_bronze(raw_rows, "stations")

    parsed = (
        batch_df
        .select(from_json(col("value").cast("string"), envelope_schema(STATION_RESULT_SCHEMA)).alias("envelope"))
        .select(explode(col("envelope.payload.results")).alias("record"))
        .select(
            col("record.stationcode").alias("station_id"),
            col("record.name").alias("name"),
            col("record.capacity").alias("capacity"),
            col("record.coordonnees_geo.lat").alias("latitude"),
            col("record.coordonnees_geo.lon").alias("longitude"),
        )
    )

    # Regles de qualite : identifiant present, capacite coherente, coordonnees dans le bbox Paris/IDF.
    valid = parsed.filter(
        col("station_id").isNotNull()
        & col("capacity").isNotNull() & (col("capacity") >= 0)
        & col("latitude").isNotNull() & col("latitude").between(48.0, 49.5)
        & col("longitude").isNotNull() & col("longitude").between(1.5, 3.0)
    )

    total = parsed.count()
    records = [row.asDict() for row in valid.collect()]
    reject_ratio = 1 - (len(records) / total) if total else 0

    if reject_ratio > REJECT_RATE_ALERT_THRESHOLD:
        log_alert(
            "TAUX_REJET_ELEVE", "WARNING",
            f"Stations : {reject_ratio:.1%} d'enregistrements rejetes par les regles de qualite",
            metric_value=reject_ratio, batch_id=batch_id,
        )

    if not records:
        return

    upsert_sql = """
        INSERT INTO stations (station_id, name, latitude, longitude, capacity, updated_at)
        VALUES (%(station_id)s, %(name)s, %(latitude)s, %(longitude)s, %(capacity)s, now())
        ON CONFLICT (station_id) DO UPDATE SET
            name = EXCLUDED.name, latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude, capacity = EXCLUDED.capacity, updated_at = now()
    """
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, records)
    logger.info("Stations : %d/%d lignes valides upsertees en base (batch_id=%s)", len(records), total, batch_id)


def process_disponibilite_batch(batch_df, epoch_id) -> None:
    if batch_df.rdd.isEmpty():
        return

    raw_rows = batch_df.select("value").collect()
    batch_id = archive_bronze(raw_rows, "disponibilite")

    parsed = (
        batch_df
        .select(from_json(col("value").cast("string"), envelope_schema(DISPONIBILITE_RESULT_SCHEMA)).alias("envelope"))
        .select(explode(col("envelope.payload.results")).alias("record"))
        .select(
            col("record.stationcode").alias("station_id"),
            col("record.numbikesavailable").alias("num_bikes_available"),
            col("record.ebike").alias("num_ebikes_available"),
            col("record.numdocksavailable").alias("num_docks_available"),
            (col("record.is_renting") == "OUI").alias("is_renting"),
            (col("record.is_returning") == "OUI").alias("is_returning"),
            col("record.duedate").alias("observed_at"),
            col("record.capacity").alias("capacity"),
            col("record.name").alias("name"),
            col("record.coordonnees_geo.lat").alias("latitude"),
            col("record.coordonnees_geo.lon").alias("longitude"),
            col("record.nom_arrondissement_communes").alias("commune"),
        )
    )

    # Regles de qualite : identifiant present, compteurs positifs, coherence vs. capacite, horodatage present.
    valid = parsed.filter(
        col("station_id").isNotNull()
        & col("num_bikes_available").isNotNull() & (col("num_bikes_available") >= 0)
        & col("num_docks_available").isNotNull() & (col("num_docks_available") >= 0)
        & col("observed_at").isNotNull()
        & (col("num_bikes_available") + col("num_docks_available") <= col("capacity") + 2)
    )

    total = parsed.count()
    rows = [row.asDict() for row in valid.collect()]
    reject_ratio = 1 - (len(rows) / total) if total else 0

    if reject_ratio > REJECT_RATE_ALERT_THRESHOLD:
        log_alert(
            "TAUX_REJET_ELEVE", "WARNING",
            f"Disponibilite : {reject_ratio:.1%} d'enregistrements rejetes par les regles de qualite",
            metric_value=reject_ratio, batch_id=batch_id,
        )

    if not rows:
        return

    max_observed = max(datetime.fromisoformat(r["observed_at"]) for r in rows)
    freshness_minutes = (datetime.now(timezone.utc) - max_observed).total_seconds() / 60
    if freshness_minutes > FRESHNESS_ALERT_THRESHOLD_MINUTES:
        log_alert(
            "FRAICHEUR_DEGRADEE", "WARNING",
            f"Donnee la plus recente vieille de {freshness_minutes:.1f} minutes",
            metric_value=freshness_minutes, batch_id=batch_id,
        )

    disponibilite_rows = [
        {k: v for k, v in r.items() if k not in ("capacity", "name", "latitude", "longitude", "commune")}
        | {"batch_id": batch_id}
        for r in rows
    ]
    insert_sql = """
        INSERT INTO disponibilite_releve
            (station_id, num_bikes_available, num_ebikes_available, num_docks_available,
             is_renting, is_returning, observed_at, batch_id)
        VALUES
            (%(station_id)s, %(num_bikes_available)s, %(num_ebikes_available)s, %(num_docks_available)s,
             %(is_renting)s, %(is_returning)s, %(observed_at)s, %(batch_id)s)
        ON CONFLICT (station_id, observed_at) DO NOTHING
    """
    # Le flux "disponibilite" porte lui-meme un referentiel minimal de chaque station
    # (name/capacity/coordonnees_geo). On s'en sert pour garantir la contrainte de cle
    # etrangere meme si le flux "stations" n'a pas encore (ou plus) cette station dans son
    # echantillon - les deux flux restent ainsi independants l'un de l'autre.
    station_stubs = [
        {"station_id": r["station_id"], "name": r["name"], "latitude": r["latitude"],
         "longitude": r["longitude"], "capacity": r["capacity"]}
        for r in rows
    ]
    stub_sql = """
        INSERT INTO stations (station_id, name, latitude, longitude, capacity, updated_at)
        VALUES (%(station_id)s, %(name)s, %(latitude)s, %(longitude)s, %(capacity)s, now())
        ON CONFLICT (station_id) DO NOTHING
    """
    communes = {(r["station_id"], r["commune"]) for r in rows if r["commune"]}
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, stub_sql, station_stubs)
            psycopg2.extras.execute_batch(cur, insert_sql, disponibilite_rows)
        if communes:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, "UPDATE stations SET commune = %s WHERE station_id = %s", list(communes)
                )
    logger.info("Disponibilite : %d/%d lignes valides inserees en base (batch_id=%s)", len(rows), total, batch_id)


def main() -> None:
    spark = SparkSession.builder.appName("velib-batch-processing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stations_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_STATIONS)
        .option("startingOffsets", "earliest")
        .load()
    )
    disponibilite_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_DISPONIBILITE)
        .option("startingOffsets", "earliest")
        .load()
    )

    query_stations = (
        stations_stream.writeStream
        .foreachBatch(process_stations_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/stations")
        .trigger(availableNow=True)
        .start()
    )
    query_disponibilite = (
        disponibilite_stream.writeStream
        .foreachBatch(process_disponibilite_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/disponibilite")
        .trigger(availableNow=True)
        .start()
    )

    query_stations.awaitTermination()
    query_disponibilite.awaitTermination()
    logger.info("=== Micro-batch VelibData termine ===")


if __name__ == "__main__":
    main()
