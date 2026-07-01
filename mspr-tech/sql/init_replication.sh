#!/bin/bash
# Execute automatiquement par l'image officielle postgres au premier demarrage du primary
# (docker-entrypoint-initdb.d, apres init_schema.sql). Cree le role de replication utilise
# par postgres-replica pour la streaming replication physique (cf. replica-entrypoint.sh).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE ${POSTGRES_REPLICATOR_USER} WITH REPLICATION LOGIN PASSWORD '${POSTGRES_REPLICATOR_PASSWORD}';
EOSQL

# Autorise la connexion de replication depuis le reseau Docker interne (postgres-replica).
echo "host replication ${POSTGRES_REPLICATOR_USER} all md5" >> "$PGDATA/pg_hba.conf"
