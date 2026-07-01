# Outillage agile et suivi de performance

## 1. Méthode agile retenue
**Scrum adapté en cycle court** (sprints d'une journée compte tenu du délai de préparation de 39h). Choix justifié par :
- Un backlog clair issu du cahier des charges (fonctionnalités F1 à F5).
- Un besoin de visibilité quotidienne sur l'avancement vu la contrainte de temps très forte.
- La possibilité de réordonner le backlog à chaque Daily Meeting (principe d'adaptation/flexibilité imposé par le sujet).

## 2. Outil de centralisation des tâches : Trello
Board **"VélibData"**, structuré en colonnes :

| Backlog | À faire (Sprint du jour) | En cours | En revue | Terminé |
|---|---|---|---|---|

- Chaque carte = une tâche du WBS ([planning.md](planning.md)), avec : responsable, estimation, checklist des critères d'acceptation.
- Étiquettes de couleur par équipe : 🔧 Data Engineering, 📊 Data Analyst, 🤖 Data Scientist, 📄 Documentation.
- Une carte "Blocages" épinglée en tête de board, mise à jour à chaque Daily Meeting.

## 3. Outil de communication : GitHub
- Dépôt Git central `velibdata` : code (`mspr-tech/`), documentation (`docs/`).
- **Issues GitHub** pour le suivi technique fin (bugs, tâches de dev), reliées aux cartes Trello correspondantes.
- **Pull Requests** obligatoires avant fusion sur `main` : revue croisée par au moins un autre membre de l'équipe avant merge (garantit la qualité et la diffusion de la connaissance).
- Convention de branches : `feature/<nom-tache>`, `fix/<nom-bug>`.

## 4. Rituels agiles
| Rituel | Fréquence | Durée | Objectif |
|---|---|---|---|
| Daily Meeting | Quotidien, 9h00 | 15 min | Avancement, blocages, réajustement du backlog |
| Revue de sprint | Fin de journée | 20 min | Démo des livrables produits, mise à jour du board |
| Rétrospective | Fin de journée | 10 min | Ce qui a bien/mal fonctionné, ajustement pour le lendemain |

## 5. Tableau de bord de suivi (indicateurs quantitatifs et qualitatifs)

| Indicateur | Type | Cible |
|---|---|---|
| % de cartes Trello "Terminé" vs planifiées du jour | Quantitatif | ≥ 90% |
| Vélocité (nb de tâches WBS closes / jour) | Quantitatif | Suivi du chemin critique (cf. planning.md) |
| Taux de disponibilité du pipeline (uptime des conteneurs Docker) | Qualitatif → mesuré | ≥ 95% |
| Taux d'enregistrements rejetés par les règles de qualité | Qualitatif → mesuré | < 2% |
| Nombre de PR revues sous 4h | Quantitatif | 100% |
| Satisfaction / clarté ressentie en rétrospective (1-5) | Qualitatif | ≥ 4/5 |

Ce tableau de bord est revu quotidiennement en Daily Meeting et alimente les ajustements de priorité (cf. [organisation_equipe.md](organisation_equipe.md), section 3).

## 6. Suivi des prestataires / fournisseurs externes
Le projet ne mobilise pas de prestataire de services informatiques externe (infrastructure auto-hébergée). Les seuls tiers du projet sont des **fournisseurs de données** (API publiques) :

| Fournisseur | Nature de la prestation | SLA connu / contrainte | Fréquence de suivi |
|---|---|---|---|
| opendata.paris.fr — API disponibilité Vélib' | Mise à disposition de données temps quasi-réel (rafraîchissement ~1 min) | Pas de SLA contractuel (API publique) → tolérance de panne côté pipeline requise | Vérifié à chaque cycle d'ingestion (alerte si non-réponse) |
| opendata.paris.fr — API stations Vélib' | Référentiel stations (localisation, capacité, statut) | Idem | Vérifié à chaque cycle d'ingestion |
| openweathermap (contrainte du sujet) | Enrichissement météo (évolution future) | Limite de quota d'appels quotidiens à respecter | À intégrer en veille technologique, non bloquant pour le MVP |

Indicateur de performance retenu pour ces fournisseurs : taux de disponibilité de l'API (mesuré par le taux d'échec des appels dans les logs du producteur Kafka) et respect du quota d'appels journaliers.
