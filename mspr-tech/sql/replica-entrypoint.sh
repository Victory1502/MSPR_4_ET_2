#!/bin/bash
# Entrypoint de postgres-replica : clone le primary via pg_basebackup au premier demarrage
# (volume vide) puis demarre Postgres en mode standby (streaming replication physique).
#
# pg_basebackup -R ecrit automatiquement standby.signal + primary_conninfo dans
# postgresql.auto.conf : la replique se connecte alors en continu au primary et rejoue le WAL.
#
# Bascule manuelle en cas de panne du primary (cf. docs/bloc4/plan_maintenance.md) :
#   docker exec mspr-tech-postgres-replica-1 pg_ctl promote -D /var/lib/postgresql/data
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "[replica-entrypoint] PGDATA vide : clonage depuis ${PRIMARY_HOST}:${PRIMARY_PORT} via pg_basebackup..."
    until pg_basebackup -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -D "$PGDATA" -U "$PGUSER" -Fp -Xs -P -R; do
        echo "[replica-entrypoint] Primary indisponible, nouvelle tentative dans 5s..."
        sleep 5
    done
    chmod 0700 "$PGDATA"
else
    echo "[replica-entrypoint] PGDATA deja initialise, demarrage direct en standby."
fi

exec docker-entrypoint.sh postgres -c hot_standby=on
