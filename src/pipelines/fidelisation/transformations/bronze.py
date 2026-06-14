# =============================================================================
# COUCHE BRONZE — Ingestion brute des tables source
# =============================================================================
# Chaque table source est importée telle quelle depuis Unity Catalog.
# Aucune transformation n'est appliquée à ce niveau.
#
# API moderne : from pyspark import pipelines as dp
# Remplace l'ancien import dlt
# =============================================================================

from pyspark import pipelines as dp

CATALOG = spark.conf.get("source_catalog", "training")
SCHEMA = spark.conf.get("source_schema", "fidelisation")


# =============================================================================
# TABLES BRONZE
# =============================================================================


@dp.table(name="bronze_clients", comment="Clients source bruts")
def bronze_clients():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.clients")


@dp.table(name="bronze_appels", comment="Historique appels source brut")
def bronze_appels():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.historique_appels")


@dp.table(name="bronze_agenda", comment="Agenda commerciaux source brut")
def bronze_agenda():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.agenda_commerciaux")


@dp.table(name="bronze_signaux", comment="Signaux contrats source brut")
def bronze_signaux():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.signaux_contrats")


@dp.table(name="bronze_heatmap", comment="Heatmap joignabilité source brut")
def bronze_heatmap():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.heatmap_joignabilite")


@dp.table(name="bronze_commerciaux", comment="Commerciaux source brut")
def bronze_commerciaux():
    return spark.read.table(f"{CATALOG}.{SCHEMA}.commerciaux")
