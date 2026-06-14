# Databricks notebook source
# DBTITLE 1,Configuration des tests
# =============================================================================
# TESTS UNITAIRES — Moteur de Fidélisation
# =============================================================================
# Vérifie la cohérence du scoring et des règles métier
# =============================================================================

from pyspark.sql import functions as F
import sys

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "fidelisation")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

results = []
def assert_test(name, condition, detail=""):
    status = "✅ PASS" if condition else "❌ FAIL"
    results.append({"test": name, "status": status, "detail": detail})
    print(f"  {status}: {name} {detail}")

# COMMAND ----------

# DBTITLE 1,Test - Integrité des tables
# =============================================================================
# TEST 1: Intégrité des tables source
# =============================================================================
print("\n" + "="*50)
print(" TEST 1: Intégrité des tables source")
print("="*50)

tables_expected = {
    "commerciaux": 20,
    "clients": 500,
    "historique_appels": 4000,  # au minimum
    "agenda_commerciaux": 1000,  # au minimum
    "signaux_contrats": 50,  # au minimum
    "heatmap_joignabilite": 40  # 5 jours x 8 créneaux
}

for table, min_rows in tables_expected.items():
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
        assert_test(
            f"Table {table} existe et non vide",
            count >= min_rows,
            f"({count} lignes, attendu >= {min_rows})"
        )
    except Exception as e:
        assert_test(f"Table {table} accessible", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test - Score d'urgence
# =============================================================================
# TEST 2: Règles de scoring urgence
# =============================================================================
print("\n" + "="*50)
print(" TEST 2: Score d'urgence")
print("="*50)

df_features = spark.table(f"{CATALOG}.{SCHEMA}.gold_priorites_clients")

# Vérifier que les scores d'urgence respectent les seuils
j15 = df_features.filter(
    (F.col("jours_avant_echeance") <= 15) & (F.col("jours_avant_echeance") >= 0)
)
if j15.count() > 0:
    min_urgence_j15 = j15.select(F.min("score_urgence")).collect()[0][0]
    assert_test(
        "J-15: score urgence >= 8",
        min_urgence_j15 >= 8,
        f"(min = {min_urgence_j15})"
    )

# Vérifier score_priorite borné
max_score = df_features.select(F.max("score_priorite")).collect()[0][0]
min_score = df_features.select(F.min("score_priorite")).collect()[0][0]
assert_test(
    "Score priorité borné raisonnablement",
    max_score is not None and max_score < 100,
    f"(min={min_score}, max={max_score})"
)

# COMMAND ----------

# DBTITLE 1,Test - Liste priorisée
# =============================================================================
# TEST 3: Liste priorisée cohérente
# =============================================================================
print("\n" + "="*50)
print(" TEST 3: Liste priorisée du jour")
print("="*50)

# Max 20 clients à appeler par région
df_jour = df_features.filter(F.col("a_appeler_aujourdhui") == True)
by_region = df_jour.groupBy("region").count().collect()
for row in by_region:
    assert_test(
        f"Région {row['region']}: max 20 appels/jour",
        row['count'] <= 20,
        f"({row['count']} clients)"
    )

# Vérifier que chaque client a un commercial assigné
nulls_commercial = df_features.filter(F.col("commercial_id").isNull()).count()
assert_test(
    "Tous les clients ont un commercial",
    nulls_commercial == 0,
    f"({nulls_commercial} sans commercial)"
)

# Vérifier que le rang est bien par région
rang_check = df_features.groupBy("region").agg(F.min("rang_priorite_region").alias("min_rang"))
all_start_at_1 = rang_check.filter(F.col("min_rang") != 1).count() == 0
assert_test(
    "Rang priorité commence à 1 par région",
    all_start_at_1
)

# COMMAND ----------

# DBTITLE 1,Test - KPIs Dashboard
# =============================================================================
# TEST 4: KPIs du dashboard
# =============================================================================
print("\n" + "="*50)
print(" TEST 4: KPIs Dashboard")
print("="*50)

try:
    df_kpi = spark.table(f"{CATALOG}.{SCHEMA}.gold_dashboard_jour")
    kpi_row = df_kpi.collect()[0]
    
    assert_test(
        "KPI nb_clients_a_appeler > 0",
        kpi_row["nb_clients_a_appeler"] > 0,
        f"({kpi_row['nb_clients_a_appeler']})"
    )
    assert_test(
        "KPI taux_joignabilite_moyen raisonnable",
        0 < kpi_row["taux_joignabilite_moyen"] < 100,
        f"({kpi_row['taux_joignabilite_moyen']}%)"
    )
    assert_test(
        "KPI ca_potentiel_en_jeu > 0",
        kpi_row["ca_potentiel_en_jeu"] > 0,
        f"({kpi_row['ca_potentiel_en_jeu']:,.0f}€)"
    )
except Exception as e:
    assert_test("Table gold_dashboard_jour accessible", False, str(e))

# COMMAND ----------

# DBTITLE 1,Résumé des tests
# =============================================================================
# RÉSUMÉ DES TESTS
# =============================================================================
print("\n" + "="*60)
print(" RÉSUMÉ DES TESTS")
print("="*60)

nb_pass = sum(1 for r in results if "✅" in r["status"])
nb_fail = sum(1 for r in results if "❌" in r["status"])
total = len(results)

print(f"\n  Total: {total} tests")
print(f"  Pass:  {nb_pass}")
print(f"  Fail:  {nb_fail}")
print("="*60)

if nb_fail > 0:
    print("\n❌ TESTS EN ÉCHEC:")
    for r in results:
        if "❌" in r["status"]:
            print(f"  - {r['test']}: {r['detail']}")
    dbutils.notebook.exit(f"FAILED: {nb_fail}/{total} tests en échec")
else:
    print(f"\n✅ TOUS LES TESTS PASSENT ({total}/{total})")
    dbutils.notebook.exit(f"SUCCESS: {total}/{total} tests passés")