# Moteur de Fidélisation Intelligent

## Description

Pipeline de priorisation intelligente des appels commerciaux pour la fidélisation client B2B.
Le système calcule un score composite basé sur l'urgence contractuelle, la joignabilité, la valeur client et les signaux marketing pour générer une liste quotidienne d'appels priorisés par commercial.

## Structure du projet

```
fidelisation/
├── databricks.yml                          # Configuration Declarative Automation Bundle
├── README.md
├── .gitignore
├── resources/
│   ├── pipelines/
│   │   └── fidelisation_pipeline.yml       # Pipeline SDP (Bronze/Silver/Gold)
│   └── jobs/
│       └── fidelisation_job.yml            # Jobs d'orchestration et de tests
├── src/
│   ├── jobs/
│   │   └── generate_data.py               # Génération de données synthétiques
│   └── pipelines/
│       └── fidelisation/
│           └── pipeline_sdp.py            # Transformations Bronze → Silver → Gold
├── tests/
│   └── test_scoring.py                    # Tests unitaires du scoring
└── docs/
    ├── architecture.md                    # Architecture technique détaillée
    ├── scoring.md                         # Algorithme de scoring expliqué
    ├── data-model.md                      # Modèle de données complet
    └── deployment.md                      # Guide de déploiement CI/CD
```

## Quickstart

```bash
# Valider la configuration
databricks bundle validate -t dev

# Déployer en dev
databricks bundle deploy -t dev

# Lancer le job complet
databricks bundle run fidelisation_job -t dev
```

## Score de priorité

| Composante | Poids | Description |
|---|---|---|
| Urgence contractuelle | 35% | Proximité échéance (J-15 à J-90) |
| Joignabilité | 30% | Taux réponse historique + heatmap |
| Valeur client | 20% | CA annuel + segment |
| Signal marketing | 15% | Offres, campagnes, saisonnalité |

## Documentation avancée

| Document | Contenu |
|----------|---------|
| [Architecture](docs/architecture.md) | Vue d'ensemble technique, flux de données, choix d'implémentation |
| [Scoring](docs/scoring.md) | Formules détaillées, pondérations, exemples de calcul |
| [Modèle de données](docs/data-model.md) | Schéma complet des tables, types, relations |
| [Déploiement](docs/deployment.md) | Guide CI/CD, environnements, troubleshooting |

## Environnements

| Target | Schéma | Mode | Schedule |
|--------|--------|------|----------|
| `dev` | fidelisation_dev | development | Manuel |
| `staging` | fidelisation_staging | development | Manuel |
| `prod` | fidelisation | production | 6h00 quotidien |

## Tests

```bash
databricks bundle run test_scoring_job -t dev
```

Les tests vérifient :
- Intégrité des tables source (volumétrie attendue)
- Règles de scoring urgence (seuils J-15/30/60/90)
- Cohérence de la liste priorisée (max 20/région, rang correct)
- KPIs du dashboard (non-null, bornes raisonnables)
