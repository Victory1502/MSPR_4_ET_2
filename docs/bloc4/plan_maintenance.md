# Plan de maintenance — VélibData

## 1. Supervision et détection des pannes

| Composant | Mécanisme de détection | Seuil / condition |
|---|---|---|
| Kafka (x3), MinIO (x4), PostgreSQL (x2), Spark master/workers (x3) | `healthcheck` Docker (`docker compose ps`) | Statut `unhealthy` après 5 tentatives |
| Cluster Kafka | kafka-exporter → Prometheus/Grafana | Sous-réplication d'une partition (< 3 répliques synchrones) |
| Réplication PostgreSQL | postgres-exporter → Grafana (`pg_replication_lag`) | Lag de réplication anormal (retard de la réplique) |
| Conteneurs (CPU/mémoire) | cAdvisor → Prometheus/Grafana | Dérive mémoire progressive (fuite), saturation CPU |
| Disponibilité des API sources | Alerte `API_INDISPONIBLE` (producteur Kafka) | Échec HTTP ou timeout > 10s |
| Qualité des données | Alerte `TAUX_REJET_ELEVE` (job Spark) | > 2 % d'enregistrements rejetés par micro-batch |
| Fraîcheur des données | Alerte `FRAICHEUR_DEGRADEE` (job Spark) | Donnée la plus récente âgée de > 10 min |
| Volumétrie anormale | Alerte `API_REPONSE_VIDE` (producteur Kafka) | 0 enregistrement renvoyé par l'API |

Toutes les alertes applicatives sont centralisées dans la table `pipeline_alertes` (PostgreSQL), consultable en SQL par toute l'équipe. Les métriques d'infrastructure (Kafka, PostgreSQL, conteneurs, Spark) sont visualisées dans le tableau de bord Grafana `VelibData - Vue d'ensemble infrastructure` (`http://localhost:3000`), cf. [architecture.md section 8](architecture.md#8-supervision-dinfrastructure-monitoring).

## 2. Procédure de maintenance curative (runbook)

| Incident | Action immédiate | Action de fond |
|---|---|---|
| Conteneur Docker `unhealthy`/arrêté | `docker compose restart <service>` puis vérifier les logs (`docker logs <service>`) | Analyser la cause racine (cf. section 4 — incidents déjà rencontrés) avant de redémarrer en boucle |
| Job Spark en échec (`StreamingQueryException`) | Le checkpoint (`/tmp/spark-checkpoints/...`) garantit qu'aucune donnée n'est perdue : corriger le code puis relancer `spark_jobs/run_job.sh`, le job reprend exactement où il s'est arrêté | Ajouter le cas en test de non-régression avant la prochaine itération |
| API source (opendata.paris.fr) indisponible | Le producteur logue l'alerte et continue son cycle suivant (pas d'arrêt du pipeline) | Vérifier le statut public de l'API, activer un mode "dernier snapshot connu" côté consommateurs si l'indisponibilité se prolonge |
| Dérive de qualité des données (> 2 % de rejets) | Consulter `pipeline_alertes` pour identifier le champ en cause, vérifier si l'API a changé de format | Mettre à jour le schéma Spark (`STATION_RESULT_SCHEMA` / `DISPONIBILITE_RESULT_SCHEMA`) si le contrat de l'API a évolué |
| Perte du volume `postgres_data` / `minio_data` | Les volumes Docker sont nommés et persistants (`docker volume ls`) — restauration depuis la dernière sauvegarde (cf. section 3) | Mettre en place une sauvegarde automatisée (`pg_dump` planifié) — actuellement non couvert par le MVP |
| Panne d'un broker Kafka (1 sur 3) | Aucune action requise : `replication.factor=3` / `min.insync.replicas=2` garantissent zéro perte de message. `docker compose ps` confirme le retour à l'état `healthy` une fois le conteneur relancé | Vérifier la ré-synchronisation des répliques (`kafka-topics --describe`) une fois le broker de retour |
| Panne d'un nœud MinIO (1 ou 2 sur 4) | Aucune action requise : l'erasure coding tolère la perte de nœuds/disques, `minio-lb` (nginx) route automatiquement vers les nœuds sains | Surveiller la reconstruction des données (auto-heal MinIO) au retour du nœud |
| Panne du primary PostgreSQL | Bascule manuelle : `docker exec mspr-tech-postgres-replica-1 pg_ctl promote -D /var/lib/postgresql/data`, puis pointer `POSTGRES_HOST` vers `postgres-replica` (cf. [architecture.md section 5](architecture.md#5-cluster-tolérant-aux-pannes-zéro-panne)) | Reconstruire un nouveau replica pointant vers l'ancien primary promu, dès que possible |

## 3. Maintenance préventive
- **Versions figées** de toutes les images Docker (cf. architecture.md) : une montée de version n'est jamais automatique, elle est testée puis actée dans une Pull Request dédiée.
- **Revue hebdomadaire** (à échelle réelle du projet, au-delà du MVP de 6 jours) des alertes accumulées dans `pipeline_alertes` pour détecter des dérives lentes (ex. taux de rejet qui augmente progressivement).
- **Sauvegarde** : `pg_dump` de la base `velibdata` avant toute modification de schéma ; les objets MinIO (couche bronze) constituent déjà une sauvegarde/rejouabilité complète des données brutes.

## 4. Incidents réels rencontrés pendant le développement (traçabilité)
Ces trois incidents ont été identifiés et corrigés pendant la construction du MVP — ils illustrent concrètement le processus de maintenance curative de l'équipe :

1. **Kafka ne démarrait jamais** (`KAFKA_PROCESS_ROLES not set`) : l'image `confluentinc/cp-kafka:latest` avait basculé vers une version qui exige le mode KRaft, incompatible avec la configuration Zookeeper historique du projet. → Migration complète vers KRaft (suppression de Zookeeper) et fixation de la version d'image.
2. **`spark-worker` crashait au démarrage** (`AccessDeniedException: /opt/spark/work`) : l'utilisateur non-root de l'image Spark n'a pas les droits d'écriture sur `/opt/spark/work`. → Redirection du répertoire de travail vers `/tmp/spark-work` via l'option `--work-dir`.
3. **Contrainte de clé étrangère violée** lors du premier traitement du flux `disponibilite` : les deux API renvoient des échantillons de stations partiellement différents. → Le job Spark upserte désormais un référentiel minimal de la station à partir des données du flux `disponibilite` lui-même (cf. modele_donnees.md, section 3).

## 5. Pistes d'amélioration

### 5.1 Réalisées pendant la préparation de la MSPR
| Amélioration | Objectif | Où |
|---|---|---|
| Cluster tolérant aux pannes (Kafka x3, MinIO x4, Postgres primary/replica, Spark x3 workers) | Zéro panne sur la perte d'un nœud | [architecture.md section 5](architecture.md#5-cluster-tolérant-aux-pannes-zéro-panne) |
| Stack de monitoring dédiée (Prometheus/Grafana + exporters + cAdvisor) | Visualiser les métriques d'infrastructure (CPU, mémoire, lag Kafka, réplication Postgres) | [architecture.md section 8](architecture.md#8-supervision-dinfrastructure-monitoring) |
| Tests automatisés (CI) sur les règles de qualité + validation de la configuration | Détecter une régression avant la mise en production, à chaque push/PR | `.github/workflows/ci.yml`, [architecture.md section 7](architecture.md#7-chaîne-dintégration-continue-cicd) |
| Orchestrateur de conteneurs (Kubernetes) avec autoscaling des workers Spark | Absorber une montée en charge sans intervention manuelle | [mspr-tech/k8s/](../../mspr-tech/k8s/README.md), [architecture.md section 9](architecture.md#9-orchestration-et-autoscaling-kubernetes) |

### 5.2 Restant au-delà du MVP
| Amélioration | Objectif |
|---|---|
| Connecteurs Spark natifs (S3A, JDBC) au lieu de boto3/psycopg2 sur le driver | Supporter des volumes par micro-batch supérieurs à la mémoire du driver |
| Ordonnanceur (Airflow/cron) déclenchant `run_job.sh` toutes les X minutes | Automatiser l'exécution périodique du micro-batch (actuellement lancée manuellement) |
| Authentification/chiffrement sur Kafka (SASL/TLS) et durcissement MinIO/Postgres (rotation des secrets) | Renforcer la sécurité au-delà du MVP local (cf. securite_rgpd.md) |
| Bascule automatique du primary PostgreSQL (Patroni/repmgr) | Remplacer la promotion manuelle de la réplique par un failover automatisé |
| Partitionnement de `disponibilite_releve` par date | Maintenir des performances de requête stables malgré la croissance du volume (~2,16M lignes/jour, cf. modele_donnees.md) |

## 6. Protocole de documentation technique
- Toute évolution d'architecture (nouveau service, changement de schéma) doit être répercutée dans `docs/bloc4/architecture.md` et `docs/bloc4/modele_donnees.md` **avant** la fusion de la Pull Request correspondante (revue croisée obligatoire, cf. outillage_agile.md).
- Le présent document (`plan_maintenance.md`) est mis à jour à chaque incident réel rencontré, sur le modèle de la section 4, afin de constituer une base de connaissance exploitable par toute personne reprenant la solution.
- Le fichier [`mspr-tech/README.md`](../../mspr-tech/README.md) sert de guide de démarrage rapide (installation, lancement, dépannage de premier niveau) et doit rester synchronisé avec le contenu réel de `docker-compose.yml`.
