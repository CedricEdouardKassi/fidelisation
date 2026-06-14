# Databricks notebook source
# DBTITLE 1,Configuration
# =============================================================================
# QUICKSTART — Déploiement du projet Fidélisation via DAB
# =============================================================================
# Ce notebook utilise le fichier databricks.yml pour déployer le bundle.
# Le CLI est téléchargé depuis GitHub (le binaire pré-installé sur serverless
# est bloqué pour un usage hors web terminal).
#
# Étapes :
#   0. Installation Databricks CLI
#   1. Validation du bundle
#   2. Déploiement
#   3. Lancement du pipeline
#   4. Lancement des tests
#   5. Destruction (optionnel)
# =============================================================================

import os

# --- Widget target ---
dbutils.widgets.dropdown("target", "dev", ["dev", "staging", "prod"], "Target environment")
TARGET = dbutils.widgets.get("target")

# --- Export des variables d'environnement pour les cellules %sh ---
PROJECT_DIR = "/Workspace/Users/cedric.kassi@openvalue.fr/fidelisation"
os.environ["TARGET"] = TARGET
os.environ["PROJECT_DIR"] = PROJECT_DIR
os.environ["PATH"] = f"/tmp/dab-cli:{os.environ.get('PATH', '')}"

print(f"🎯 Target:     {TARGET}")
print(f"📁 Project:    {PROJECT_DIR}")
print(f"✅ Variables d'environnement exportées pour %sh")

# COMMAND ----------

# DBTITLE 1,1. Validate Bundle
# MAGIC %undefined
# MAGIC # =============================================================================
# MAGIC # Installation du Databricks CLI depuis GitHub releases
# MAGIC # (le binaire pré-installé sur serverless est restreint aux web terminals)
# MAGIC # =============================================================================
# MAGIC
# MAGIC INSTALL_DIR="/tmp/dab-cli"
# MAGIC import subprocesssubprocess.run(['mkdir', '-p', INSTALL_DIR], check=True)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Téléchargement du CLI officiel
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh -s -- --install-dir $INSTALL_DIR 2>&1
# MAGIC
# MAGIC # Vérification
# MAGIC export PATH="$INSTALL_DIR:$PATH"
# MAGIC databricks --version
# MAGIC
# MAGIC echo ""
# MAGIC echo "✅ Databricks CLI installé dans $INSTALL_DIR"

# COMMAND ----------

# DBTITLE 1,2. Deploy Bundle
# MAGIC %undefined
# MAGIC # =============================================================================
# MAGIC # VALIDATION — Vérifie la cohérence du bundle (databricks.yml)
# MAGIC # =============================================================================
# MAGIC
# MAGIC export PATH="/tmp/dab-cli:$PATH"
# MAGIC cd $PROJECT_DIR
# MAGIC
# MAGIC echo "📋 Validation du bundle pour le target: $TARGET"
# MAGIC echo "   Projet: $PROJECT_DIR"
# MAGIC echo ""
# MAGIC
# MAGIC databricks bundle validate -t $TARGET

# COMMAND ----------

# DBTITLE 1,3. Run Pipeline
# MAGIC %undefined
# MAGIC # =============================================================================
# MAGIC # DÉPLOIEMENT — Déploie les ressources définies dans databricks.yml
# MAGIC # =============================================================================
# MAGIC
# MAGIC export PATH="/tmp/dab-cli:$PATH"
# MAGIC cd $PROJECT_DIR
# MAGIC
# MAGIC echo "🚀 Déploiement du bundle sur [$TARGET]..."
# MAGIC echo ""
# MAGIC
# MAGIC databricks bundle deploy -t $TARGET --auto-approve

# COMMAND ----------

# DBTITLE 1,4. Run Tests
# MAGIC %undefined
# MAGIC # =============================================================================
# MAGIC # LANCEMENT DU PIPELINE — Déclenche la mise à jour du pipeline SDP
# MAGIC # =============================================================================
# MAGIC
# MAGIC export PATH="/tmp/dab-cli:$PATH"
# MAGIC cd $PROJECT_DIR
# MAGIC
# MAGIC echo "▶️  Lancement du pipeline fidelisation_pipeline sur [$TARGET]..."
# MAGIC echo ""
# MAGIC
# MAGIC databricks bundle run fidelisation_pipeline -t $TARGET

# COMMAND ----------

# DBTITLE 1,5. Destroy (cleanup)
# MAGIC %undefined
# MAGIC # =============================================================================
# MAGIC # LANCEMENT DES TESTS — Exécute le job de tests unitaires
# MAGIC # =============================================================================
# MAGIC
# MAGIC export PATH="/tmp/dab-cli:$PATH"
# MAGIC cd $PROJECT_DIR
# MAGIC
# MAGIC echo "🧪 Lancement des tests sur [$TARGET]..."
# MAGIC echo ""
# MAGIC
# MAGIC databricks bundle run test_scoring_job -t $TARGET

# COMMAND ----------

# DBTITLE 1,5. Destroy (cleanup)
# MAGIC %undefined
# MAGIC # =============================================================================
# MAGIC # DESTROY — Supprime toutes les ressources déployées
# MAGIC # =============================================================================
# MAGIC # ⚠️ ATTENTION: Décommentez et exécutez uniquement si nécessaire
# MAGIC # =============================================================================
# MAGIC
# MAGIC # export PATH="/tmp/dab-cli:$PATH"
# MAGIC # cd $PROJECT_DIR
# MAGIC #
# MAGIC # echo "⚠️  Destruction des ressources sur [$TARGET]..."
# MAGIC # databricks bundle destroy -t $TARGET --auto-approve