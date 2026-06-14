# Architecture Technique

## Vue d'ensemble

Le Moteur de Fidélisation Intelligent s'appuie sur une architecture **Medallion** (Bronze → Silver → Gold) implémentée en tant que Spark Declarative Pipeline (SDP) sur Databricks.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION (Job)                          │
├────────────────────────────┬────────────────────────────────────────┤
│   Task 1: generate_data    │   Task 2: Pipeline SDP                │
│   (Notebook serverless)    │   (Bronze → Silver → Gold)            │
└────────────────────────────┴────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
            ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
            │    BRONZE    │   │    SILVER    │   │     GOLD     │
            │  6 tables    │   │  1 table     │   │  2 tables    │
            │  brutes      │   │  features    │   │  scorées     │
            └──────────────┘   └──────────────┘   └──────────────┘
```

## Composants

### 1. Génération de données (`src/jobs/generate_data`)

Notebook autonome qui crée les 6 tables source dans Unity Catalog :

| Table | Volume | Description |
|-------|--------|-------------|
| `commerciaux` | 20 lignes | Équipe commerciale avec régions |
| `clients` | 500 lignes | Portefeuille B2B avec contrats |
| `historique_appels` | ~5000 lignes | 12 mois d'historique |
| `agenda_commerciaux` | ~3000 lignes | Disponibilités J+30 |
| `signaux_contrats` | ~600 lignes | Signaux déclencheurs |
| `heatmap_joignabilite` | 40 lignes | Matrice jour×heure |

### 2. Pipeline SDP (`src/pipelines/fidelisation/`)

Pipeline serverless en 3 couches :

- **Bronze** : Ingestion directe des tables Unity Catalog (lecture batch)
- **Silver** : Agrégation des features client (appels, signaux, scores partiels)
- **Gold** : Score composite pondéré + liste priorisée du jour

### 3. Orchestration (`resources/jobs/`)

Le job exécute séquentiellement :
1. Génération/rafraîchissement des données
2. Mise à jour du pipeline (mode triggered)

Schedule : tous les jours à 6h (Europe/Paris).

## Flux de données

```
[Tables source UC] ──► [Bronze: copie brute]
                              │
                              ▼
                       [Silver: enrichissement]
                       - Agrégation appels 12 mois
                       - Taux réponse historique
                       - Score urgence (J-15/30/60/90)
                       - Score joignabilité
                       - Score valeur client
                       - Score signal marketing
                              │
                              ▼
                       [Gold: scoring final]
                       - Score composite pondéré
                       - Rang par région
                       - Meilleur créneau (agenda × heatmap)
                       - Flag "à_appeler_aujourd'hui"
```

## Choix techniques

| Aspect | Choix | Justification |
|--------|-------|---------------|
| Compute | Serverless | Pas de gestion de cluster, scaling auto |
| Pipeline | SDP (Declarative) | Gestion des dépendances, lineage, DQ |
| Photon | Activé | Performance sur les agrégations |
| Stockage | Unity Catalog | Gouvernance, lineage, partage |
| Paramétrage | Variables DAB | Multi-environnement (dev/staging/prod) |
