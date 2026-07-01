"""
Regles de qualite des donnees VelibData (cf. cahier des charges fonctionnel, section 2.3).

Module volontairement independant de PySpark/Kafka/MinIO/Postgres : ce sont des fonctions
Python pures, importables et testables sans JVM ni infrastructure (cf. tests/test_quality_rules.py,
execute a chaque push/pull request dans la chaine CI - .github/workflows/ci.yml).

Ces constantes et fonctions sont la source de verite unique : les filtres Spark (colonnes)
appliques dans velib_batch_processing.py reprennent exactement les memes seuils.
"""
from typing import Optional

REJECT_RATE_ALERT_THRESHOLD = 0.02
FRESHNESS_ALERT_THRESHOLD_MINUTES = 10
PARIS_LAT_RANGE = (48.0, 49.5)
PARIS_LON_RANGE = (1.5, 3.0)
CAPACITY_TOLERANCE = 2  # marge admise entre (velos + docks) et capacite affichee de la station


def is_station_valid(
    station_id: Optional[str],
    capacity: Optional[int],
    latitude: Optional[float],
    longitude: Optional[float],
) -> bool:
    """Identifiant present, capacite coherente, coordonnees dans le bbox Paris/IDF."""
    if station_id is None:
        return False
    if capacity is None or capacity < 0:
        return False
    if latitude is None or not (PARIS_LAT_RANGE[0] <= latitude <= PARIS_LAT_RANGE[1]):
        return False
    if longitude is None or not (PARIS_LON_RANGE[0] <= longitude <= PARIS_LON_RANGE[1]):
        return False
    return True


def is_disponibilite_valid(
    station_id: Optional[str],
    num_bikes_available: Optional[int],
    num_docks_available: Optional[int],
    observed_at: Optional[str],
    capacity: Optional[int],
) -> bool:
    """Identifiant present, compteurs positifs, coherence vs. capacite, horodatage present."""
    if station_id is None:
        return False
    if num_bikes_available is None or num_bikes_available < 0:
        return False
    if num_docks_available is None or num_docks_available < 0:
        return False
    if observed_at is None:
        return False
    if capacity is not None and (num_bikes_available + num_docks_available) > capacity + CAPACITY_TOLERANCE:
        return False
    return True
