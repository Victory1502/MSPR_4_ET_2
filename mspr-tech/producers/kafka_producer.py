"""
Producteur Kafka VelibData.

Remplace l'ancien script `velib_to_minio.py` qui ecrivait directement dans MinIO
en contournant Kafka. Ici, le producteur ne fait qu'une chose : interroger les
2 API Velib' et publier les payloads bruts sur des topics Kafka. La transformation,
le controle qualite et le stockage (MinIO + PostgreSQL) sont delegues au job Spark
(cf. spark_jobs/velib_batch_processing.py) - separation claire ingestion / traitement.

Contrainte du sujet respectee : "il ne faut pas pousser l'aspect temps reel trop loin"
-> polling toutes les FETCH_INTERVAL_SECONDS (par defaut 60s), pas de vrai streaming continu.
"""
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kafka_producer")


@dataclass(frozen=True)
class ProducerConfig:
    """Resolue une seule fois dans main() : evite de lire l'environnement a l'import du
    module (import-time side effects), ce qui permet d'importer ce fichier depuis les tests
    sans avoir toutes les variables d'environnement de production definies."""
    kafka_bootstrap_servers: str
    topic_stations: str
    topic_disponibilite: str
    fetch_interval_seconds: int
    postgres_dsn: str


def load_config() -> ProducerConfig:
    load_dotenv()
    return ProducerConfig(
        # Cote hote (script lance hors Docker) : les 3 ports exposes des 3 brokers Kafka.
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
        ),
        topic_stations=os.environ.get("KAFKA_TOPIC_STATIONS", "velib.stations.raw"),
        topic_disponibilite=os.environ.get("KAFKA_TOPIC_DISPONIBILITE", "velib.disponibilite.raw"),
        fetch_interval_seconds=int(os.environ.get("FETCH_INTERVAL_SECONDS", "60")),
        postgres_dsn=(
            f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
            f"port={os.environ.get('POSTGRES_PORT', '5432')} "
            f"dbname={os.environ['POSTGRES_DB']} "
            f"user={os.environ['POSTGRES_USER']} "
            f"password={os.environ['POSTGRES_PASSWORD']}"
        ),
    )


URL_STATIONS = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-emplacement-des-stations/records?limit=100"
)
URL_DISPONIBILITE = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/records?limit=100"
)


def log_alert(cfg: ProducerConfig, alerte_type: str, severity: str, description: str,
              metric_value: float = None) -> None:
    """Trace une anomalie a la fois dans les logs et dans la table de supervision pipeline_alertes."""
    logger.warning("ALERTE [%s/%s] %s", severity, alerte_type, description)
    try:
        with psycopg2.connect(cfg.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_alertes (alerte_type, severity, description, metric_value)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (alerte_type, severity, description, metric_value),
                )
    except Exception:
        logger.exception("Impossible d'ecrire l'alerte dans PostgreSQL (pipeline_alertes)")


def fetch_data(cfg: ProducerConfig, url: str, source_name: str) -> dict | None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        nb_records = len(data.get("results", []))
        logger.info("%s : %d enregistrements recuperes", source_name, nb_records)
        if nb_records == 0:
            log_alert(cfg, "API_REPONSE_VIDE", "WARNING", f"{source_name} a renvoye 0 enregistrement")
        return data
    except requests.RequestException as exc:
        log_alert(cfg, "API_INDISPONIBLE", "CRITICAL", f"Echec appel {source_name} : {exc}")
        return None


def publish(producer: KafkaProducer, topic: str, source_name: str, payload: dict) -> None:
    envelope = {
        "source": source_name,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": str(uuid.uuid4()),
        "payload": payload,
    }
    producer.send(topic, value=envelope)
    producer.flush()
    logger.info("Publie sur le topic '%s' (batch_id=%s)", topic, envelope["batch_id"])


def main() -> None:
    cfg = load_config()
    logger.info("=== Producteur Kafka VelibData - intervalle %ss ===", cfg.fetch_interval_seconds)
    producer = KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    try:
        while True:
            stations_data = fetch_data(cfg, URL_STATIONS, "stations")
            if stations_data:
                publish(producer, cfg.topic_stations, "stations", stations_data)

            dispo_data = fetch_data(cfg, URL_DISPONIBILITE, "disponibilite")
            if dispo_data:
                publish(producer, cfg.topic_disponibilite, "disponibilite", dispo_data)

            time.sleep(cfg.fetch_interval_seconds)
    except KeyboardInterrupt:
        logger.info("Arret demande par l'utilisateur.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
