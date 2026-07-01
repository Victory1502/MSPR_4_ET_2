"""
Tests unitaires des regles de qualite du job Spark (is_station_valid / is_disponibilite_valid).

Ces fonctions sont pures (aucun appel Spark/Kafka/MinIO/Postgres, aucune variable
d'environnement requise) : executees dans la chaine CI (.github/workflows/ci.yml) a
chaque push/pull request, sans avoir besoin de demarrer l'infrastructure Docker.
"""
from spark_jobs.quality_rules import (
    CAPACITY_TOLERANCE,
    is_disponibilite_valid,
    is_station_valid,
)

PARIS_STATION = dict(station_id="16107", capacity=30, latitude=48.8566, longitude=2.3522)


def test_station_valid_nominal():
    assert is_station_valid(**PARIS_STATION) is True


def test_station_invalid_missing_id():
    params = {**PARIS_STATION, "station_id": None}
    assert is_station_valid(**params) is False


def test_station_invalid_negative_capacity():
    params = {**PARIS_STATION, "capacity": -1}
    assert is_station_valid(**params) is False


def test_station_invalid_missing_capacity():
    params = {**PARIS_STATION, "capacity": None}
    assert is_station_valid(**params) is False


def test_station_invalid_outside_paris_bbox():
    # Marseille (~43.3N) est hors du bbox Paris/IDF retenu.
    params = {**PARIS_STATION, "latitude": 43.2965}
    assert is_station_valid(**params) is False


def test_station_invalid_missing_coordinates():
    params = {**PARIS_STATION, "latitude": None, "longitude": None}
    assert is_station_valid(**params) is False


DISPONIBILITE_OK = dict(
    station_id="16107",
    num_bikes_available=10,
    num_docks_available=15,
    observed_at="2026-07-01T10:00:00+00:00",
    capacity=25,
)


def test_disponibilite_valid_nominal():
    assert is_disponibilite_valid(**DISPONIBILITE_OK) is True


def test_disponibilite_invalid_missing_id():
    params = {**DISPONIBILITE_OK, "station_id": None}
    assert is_disponibilite_valid(**params) is False


def test_disponibilite_invalid_negative_bikes():
    params = {**DISPONIBILITE_OK, "num_bikes_available": -1}
    assert is_disponibilite_valid(**params) is False


def test_disponibilite_invalid_missing_timestamp():
    params = {**DISPONIBILITE_OK, "observed_at": None}
    assert is_disponibilite_valid(**params) is False


def test_disponibilite_invalid_incoherent_with_capacity():
    # 50 velos + 50 docks pour une capacite de 25 : tres largement incoherent.
    params = {**DISPONIBILITE_OK, "num_bikes_available": 50, "num_docks_available": 50}
    assert is_disponibilite_valid(**params) is False


def test_disponibilite_valid_within_capacity_tolerance():
    # bikes + docks == capacity + CAPACITY_TOLERANCE (borne acceptee, cf. bruit de mesure API).
    params = {
        **DISPONIBILITE_OK,
        "num_bikes_available": DISPONIBILITE_OK["capacity"] + CAPACITY_TOLERANCE,
        "num_docks_available": 0,
    }
    assert is_disponibilite_valid(**params) is True


def test_disponibilite_valid_without_known_capacity():
    # Capacite non renseignee (None) : la regle de coherence ne s'applique pas.
    params = {**DISPONIBILITE_OK, "capacity": None, "num_bikes_available": 999}
    assert is_disponibilite_valid(**params) is True
