# Guide de Déploiement

## Prérequis

| Outil | Version minimale | Vérification |
|-------|-----------------|--------------|
| Databricks CLI | ≥ 0.281.0 | `databricks --version` |
| Accès Unity Catalog | Catalogue `training` | `databricks catalogs list` |
| Profil configuré | `.databrickscfg` | `databricks auth profiles` |

## Configuration des profils

### Fichier `~/.databrickscfg`

```ini
[dev]
host  = https://adb-1400176502464428.8.azuredatabricks.net
token = dapi_xxxxxxxx

[prod]
host  = https://adb-1400176502464428.8.azuredatabricks.net
token = dapi_yyyyyyyy
```

## Workflow de déploiement

### 1. Validation

```bash
# Valider la syntaxe et les références
databricks bundle validate -t dev
databricks bundle validate -t prod
```

Erreurs courantes :
- `variable not found` → vérifier `databricks.yml`
- `resource not found` → vérifier les chemins relatifs dans `resources/`
- `permission denied` → vérifier l'accès au catalogue cible

### 2. Déploiement en développement

```bash
# Déployer toutes les ressources en dev
databricks bundle deploy -t dev

# Vérifier le déploiement
databricks bundle summary -t dev
```

En mode `development`, les ressources sont préfixées avec `[dev]` et le pipeline est en mode développement (pas de scheduling automatique).

### 3. Exécution manuelle

```bash
# Lancer le job complet (generate + pipeline)
databricks bundle run fidelisation_job -t dev

# Lancer uniquement le pipeline
databricks bundle run fidelisation_pipeline -t dev

# Lancer les tests
databricks bundle run test_scoring_job -t dev
```

### 4. Promotion en staging

```bash
# Déployer en staging
databricks bundle deploy -t staging

# Exécuter les tests
databricks bundle run test_scoring_job -t staging

# Vérifier les résultats
databricks bundle run fidelisation_job -t staging
```

### 5. Déploiement en production

```bash
# Déployer en production (confirmation requise)
databricks bundle deploy -t prod

# Le schedule démarre automatiquement (6h Europe/Paris)
```

## Environnements

| Target | Catalogue | Schéma | Mode | Schedule |
|--------|-----------|--------|------|----------|
| `dev` | training | fidelisation_dev | development | Manuel |
| `staging` | training | fidelisation_staging | development | Manuel |
| `prod` | training | fidelisation | production | 6h00 quotidien |

## Rollback

```bash
# Supprimer les ressources déployées
databricks bundle destroy -t dev --auto-approve

# Redéployer une version antérieure
git checkout <commit_hash>
databricks bundle deploy -t prod
```

## CI/CD (GitHub Actions)

Exemple de pipeline CI/CD :

```yaml
# .github/workflows/deploy.yml
name: Deploy Fidélisation

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t staging

  test:
    needs: validate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: |
          databricks bundle deploy -t staging
          databricks bundle run test_scoring_job -t staging

  deploy-prod:
    if: github.ref == 'refs/heads/main'
    needs: test
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t prod --auto-approve
```

## Troubleshooting

| Problème | Cause probable | Solution |
|----------|---------------|----------|
| Pipeline échoue au Bronze | Tables source manquantes | Exécuter `generate_data` d'abord |
| Score NULL dans Gold | Pas de signaux pour certains clients | Vérifier `signaux_contrats` |
| Job timeout | Cluster lent au démarrage | Passer en serverless |
| Permission denied sur schema | Droits UC insuffisants | `GRANT USE SCHEMA ON ...` |
