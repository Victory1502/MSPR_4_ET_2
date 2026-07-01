#!/usr/bin/env bash
# Lance le job de traitement VelibData a l'interieur du conteneur spark-master.
# A executer depuis le dossier mspr-tech/, avec la stack docker compose deja demarree.
set -euo pipefail

# Evite que Git Bash/MSYS ne reecrive les chemins "/opt/..." en chemins Windows
# lorsqu'ils sont passes en argument a docker.exe.
export MSYS_NO_PATHCONV=1

# HOME=/tmp : l'utilisateur non-root de l'image n'a pas le droit d'ecrire dans /opt/spark,
# on redirige donc le "user site" pip (~/.local) vers un repertoire inscriptible.
docker exec -e HOME=/tmp spark-master pip install --quiet boto3 psycopg2-binary

# --conf spark.jars.ivy : /opt/spark n'est pas inscriptible par l'utilisateur non-root de
# l'image, le cache Ivy utilise pour resoudre --packages est donc redirige vers /tmp.
docker exec -e HOME=/tmp spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark-apps/spark_jobs/velib_batch_processing.py
