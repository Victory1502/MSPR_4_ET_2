-- Modele de donnees normalise (3NF) pour VelibData
-- Couche "silver" : donnees nettoyees, dedupliquees, requetables par les Data Analysts / Data Scientists.
-- La couche "bronze" (JSON brut horodate) reste stockee dans MinIO pour l'historisation complete (rejouabilite).

-- Dimension : referentiel des stations
CREATE TABLE IF NOT EXISTS stations (
    station_id      VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    capacity        INTEGER NOT NULL CHECK (capacity >= 0),
    commune         VARCHAR(128),
    is_installed    BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- Fait : releves de disponibilite horodates (serie temporelle, volume croissant)
CREATE TABLE IF NOT EXISTS disponibilite_releve (
    id                      BIGSERIAL PRIMARY KEY,
    station_id              VARCHAR(64) NOT NULL REFERENCES stations(station_id),
    num_bikes_available     INTEGER NOT NULL CHECK (num_bikes_available >= 0),
    num_ebikes_available    INTEGER NOT NULL DEFAULT 0 CHECK (num_ebikes_available >= 0),
    num_docks_available     INTEGER NOT NULL CHECK (num_docks_available >= 0),
    is_renting              BOOLEAN NOT NULL,
    is_returning            BOOLEAN NOT NULL,
    observed_at             TIMESTAMP NOT NULL,   -- horodatage fourni par l'API source
    ingested_at             TIMESTAMP NOT NULL DEFAULT now(),
    batch_id                VARCHAR(64) NOT NULL, -- identifiant du micro-batch Spark ayant ecrit la ligne
    UNIQUE (station_id, observed_at)              -- evite les doublons en cas de re-ingestion
);

CREATE INDEX IF NOT EXISTS idx_disponibilite_station_time
    ON disponibilite_releve (station_id, observed_at DESC);

-- Supervision : alertes de qualite / disponibilite du pipeline
CREATE TABLE IF NOT EXISTS pipeline_alertes (
    id              BIGSERIAL PRIMARY KEY,
    alerte_type     VARCHAR(64) NOT NULL,   -- ex: 'SCHEMA_INVALIDE', 'TAUX_REJET_ELEVE', 'API_INDISPONIBLE', 'FRAICHEUR_DEGRADEE'
    severity        VARCHAR(16) NOT NULL CHECK (severity IN ('INFO','WARNING','CRITICAL')),
    description     TEXT NOT NULL,
    metric_value    DOUBLE PRECISION,
    batch_id        VARCHAR(64),
    detected_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alertes_severity_time
    ON pipeline_alertes (severity, detected_at DESC);
