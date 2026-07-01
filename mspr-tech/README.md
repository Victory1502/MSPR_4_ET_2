# VélibData — mspr-tech

Infrastructure et pipelines de données du projet VélibData. Vue d'ensemble de l'architecture : [../docs/bloc4/architecture.md](../docs/bloc4/architecture.md).

## 1. Prérequis
- Docker Desktop (Compose v2)
- Python 3.11+ (un environnement virtuel est déjà présent dans ce dossier)

## 2. Démarrage rapide

```bash
# 1. Configurer les secrets (jamais commités)
cp .env.example .env
# -> éditer .env avec des identifiants propres à votre poste

# 2. Démarrer l'infrastructure (Kafka, MinIO, PostgreSQL, Spark)
docker compose up -d
docker compose ps   # tous les services doivent passer "healthy"

# 3. Installer les dépendances Python (producteur + utilitaires)
python -m venv .   # si l'environnement n'existe pas déjà
./Scripts/pip install -r requirements.txt   # Windows : Scripts/python.exe -m pip install -r requirements.txt

# 4. Lancer le producteur (ingestion continue des 2 API Vélib')
python producers/kafka_producer.py

# 5. Dans un autre terminal : lancer le traitement Spark (micro-batch à la demande)
bash spark_jobs/run_job.sh
```

## 3. Vérifier que les données sont bien arrivées

```bash
# Données normalisées (couche silver)
docker exec mspr-tech-postgres-1 psql -U velib_app -d velibdata -c "SELECT count(*) FROM stations;"
docker exec mspr-tech-postgres-1 psql -U velib_app -d velibdata -c "SELECT count(*) FROM disponibilite_releve;"
docker exec mspr-tech-postgres-1 psql -U velib_app -d velibdata -c "SELECT * FROM pipeline_alertes ORDER BY detected_at DESC LIMIT 5;"

# Archive brute (couche bronze) - nécessite le client mc, déjà inclus dans l'image minio
docker exec mspr-tech-minio-1 mc alias set local http://localhost:9000 <MINIO_ROOT_USER> <MINIO_ROOT_PASSWORD>
docker exec mspr-tech-minio-1 mc ls --recursive local/velib-data
```

Interfaces web : MinIO Console `http://localhost:9001`, Spark Master UI `http://localhost:8080`.

## 4. Structure du dossier

```
mspr-tech/
├── docker-compose.yml       # Kafka (KRaft), MinIO, PostgreSQL, Spark master/worker
├── requirements.txt         # Dépendances Python (producteur + job Spark côté driver)
├── .env.example             # Modèle de configuration (copier en .env, jamais commité)
├── producers/
│   └── kafka_producer.py    # Ingestion : API Vélib' -> topics Kafka
├── spark_jobs/
│   ├── velib_batch_processing.py  # Transformation, qualité, alertes, écriture MinIO+Postgres
│   └── run_job.sh                 # Lance le job dans le conteneur spark-master
└── sql/
    └── init_schema.sql      # Modèle de données normalisé (chargé au démarrage de Postgres)
```

## 5. En cas de problème
Voir le runbook de maintenance curative : [../docs/bloc4/plan_maintenance.md](../docs/bloc4/plan_maintenance.md).
