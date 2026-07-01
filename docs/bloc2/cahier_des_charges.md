# Cahier des charges — Projet VélibData

## 1. Cahier des charges technique

### 1.1 Objectifs
- Construire une plateforme Big Data capable de collecter, stocker et traiter en quasi temps réel les données Vélib' (disponibilité des vélos + informations stations) issues des API opendata.paris.fr.
- Garantir une donnée fiable, historisée, normalisée et exploitable par les équipes Data Analysts et Data Scientists.
- Assurer la sécurité et la conformité RGPD (hébergement UE/France, accès contrôlé).
- Livrer un MVP fonctionnel de bout en bout, puis l'améliorer par itérations (approche agile imposée par le sujet).

### 1.2 Ressources planifiées
| Ressource | Détail |
|---|---|
| Humaines | 4 apprenants : 1 Data Engineer (Victory), 2 Data Analysts (Belkis, Lucas), 1 Data Scientist (Lyes) |
| Techniques | Docker Desktop, Apache Kafka + Zookeeper, MinIO (S3-compatible), Apache Spark (master/worker), GitHub, Trello |
| Financières | Solution 100% open-source / auto-hébergée en local pour la durée du projet → coût infrastructure nul (hors temps humain). Pas de licence propriétaire. |
| Temporelles | Préparation : du 01/07/2026 au 06/07/2026 (soutenance) — voir [planning.md](planning.md) |

### 1.3 Outils d'évaluation
- Revue de code via Pull Requests GitHub.
- Tests manuels de bout en bout du pipeline (API → Kafka → Spark → MinIO) avant chaque itération.
- Vérification des règles de qualité de données (voir [modèle de données](../bloc4/modele_donnees.md)) sur chaque lot ingéré.
- Daily Meeting (15 min) pour vérifier l'avancement par rapport au planning.

### 1.4 Mise en œuvre
- Méthode agile **Scrum** adaptée en cycle court (sprints de 1 jour compte tenu du délai de préparation de 39h / 5 jours calendaires).
- Board **Trello** pour le suivi des tâches (colonnes : Backlog / En cours / En revue / Terminé).
- **GitHub** comme dépôt de code + Issues pour le suivi technique + Pull Requests pour la revue.
- Environnement Docker Compose reproductible par tous les membres de l'équipe.

---

## 2. Cahier des charges fonctionnel

### 2.1 Objectifs des directions métiers
La ville de Paris (VélibData) souhaite :
- Anticiper les pics de demande et optimiser la répartition des vélos entre stations.
- Permettre aux Data Analysts de suivre en temps réel l'utilisation du service (tableaux de bord).
- Permettre aux Data Scientists de développer des modèles de prédiction et de recommandation à partir de données fiables et historisées.

### 2.2 Fonctionnalités
| # | Fonctionnalité | Équipe bénéficiaire |
|---|---|---|
| F1 | Ingestion automatisée des 2 API Vélib' (stations, disponibilité) | Data Engineer |
| F2 | Historisation systématique des données brutes et transformées | Toutes |
| F3 | Contrôle qualité des données + alertes en cas d'anomalie | Data Engineer |
| F4 | Mise à disposition de données normalisées et requêtables | Data Analysts / Data Scientists |
| F5 | Supervision de la disponibilité des pipelines | Data Engineer |

### 2.3 Indicateurs de performance (KPI)
- Taux de disponibilité du pipeline d'ingestion (objectif MVP : ≥ 95% sur la période de test).
- Latence entre publication API et disponibilité dans le datalake (objectif : < 5 min, cf. contrainte « ne pas pousser l'aspect temps réel trop loin »).
- Taux d'enregistrements rejetés par les règles de qualité (objectif : < 2%).
- Nombre d'alertes qualité déclenchées et traitées.

### 2.4 Dates clés des livrables
Voir le détail dans [planning.md](planning.md) — jalons principaux :
- **J1 (01/07)** : Cadrage, architecture cible, cahier des charges.
- **J2-J3 (02-03/07)** : Pipeline Kafka → Spark → MinIO fonctionnel, modèle de données, règles de qualité.
- **J4 (04/07)** : Documentation technique, plan de maintenance, sécurité.
- **J5 (05/07)** : Finalisation des livrables, répétition de la soutenance.
- **J6 (06/07)** : Soutenance orale.
