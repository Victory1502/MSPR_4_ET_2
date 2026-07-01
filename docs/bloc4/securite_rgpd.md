# Sécurité et conformité RGPD

## 1. Localisation des données (contrainte III du sujet)
Toute la plateforme (Kafka, MinIO, PostgreSQL, Spark) est **auto-hébergée en local via Docker Compose**, sur une machine physiquement située en France pour la durée du projet. Aucune donnée ne transite vers un service tiers hors UE. En cas de migration vers un hébergement cloud (piste d'évolution), le choix se porterait sur une région UE d'un fournisseur (ex. `eu-west-3` Paris chez AWS, ou un hébergeur français comme OVHcloud/Scaleway) afin de conserver cette garantie.

## 2. Nature des données traitées
Les données Vélib' collectées (identifiant de station, localisation, capacité, disponibilité de vélos) sont des **données ouvertes publiques** (opendata.paris.fr) et ne contiennent aucune donnée personnelle au sens RGPD (pas d'identifiant utilisateur, pas de trajet individuel). Le risque RGPD porte donc principalement sur la **sécurisation de l'infrastructure elle-même** plutôt que sur la nature des données.

## 3. Gestion des secrets
- Aucun identifiant/mot de passe n'est écrit en dur dans le code ou dans `docker-compose.yml` : toutes les valeurs sensibles (`MINIO_ROOT_PASSWORD`, `POSTGRES_PASSWORD`, etc.) sont lues depuis un fichier `.env` **non versionné** (cf. `.gitignore`), à partir du modèle fourni dans `.env.example`.
- Avant correction, le dépôt d'origine contenait des identifiants MinIO en clair (`minioadmin` / `minioadmin`) directement dans `docker-compose.yml` et dans `velib_to_minio.py` — corrigé lors de la refonte du pipeline (cf. plan_maintenance.md, section 4).
- Les valeurs par défaut visibles dans `docker-compose.yml` (`${MINIO_ROOT_PASSWORD:-minioadmin}`) ne servent que de garde-fou si `.env` est absent ; elles ne doivent jamais être utilisées en dehors d'un poste de développement isolé.

## 4. Accès sécurisé
| Composant | Etat actuel (MVP) | Limite connue | Piste d'évolution |
|---|---|---|---|
| MinIO | Authentification par identifiants dédiés (non `minioadmin`), accès limité à `localhost` | Pas de chiffrement TLS en local | Activer TLS + politiques IAM par bucket si exposition réseau au-delà de `localhost` |
| PostgreSQL | Utilisateur applicatif dédié (`velib_app`), pas de compte `postgres` superutilisateur exposé | Pas de chiffrement des connexions (`sslmode`) | Activer `sslmode=require` en environnement partagé |
| Kafka | Accès réseau limité à `localhost`/réseau Docker interne (`velib-network`) | Pas d'authentification SASL, pas de chiffrement | Ajouter SASL/TLS avant toute exposition hors du poste de développement |
| Réseau | Tous les services communiquent sur un réseau Docker dédié (`velib-network`), isolé du reste de l'hôte | — | — |

Ce tableau est volontairement transparent sur les limites du MVP : le sujet impose une **livraison itérative** (« livrer un MVP qui sera amélioré progressivement ») — le durcissement réseau (SASL/TLS, IAM) est identifié comme la prochaine itération de sécurité plutôt que traité superficiellement dans les délais impartis.

## 5. Traçabilité et audit
- Toute anomalie de qualité ou de disponibilité est historisée dans `pipeline_alertes` avec horodatage — permet de reconstituer un historique d'incidents en cas d'audit.
- La couche bronze MinIO conserve une copie brute immuable de chaque message ingéré : en cas de doute sur une transformation, la donnée source exacte reste consultable et ré-exécutable.
