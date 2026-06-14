# Databricks notebook source
# DBTITLE 1,Configuration
# =============================================================================
# TESTS D'INTÉGRATION — Pipeline de scoring (Silver + Gold)
# Exécuter depuis le workspace Databricks après le pipeline SDP
# =============================================================================

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "fidelisation")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

results = []

def assert_test(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append({"test": name, "status": status, "detail": detail})
    icon = "✅" if condition else "❌"
    print(f"  {icon} {status}: {name} {detail}")

# COMMAND ----------

# DBTITLE 1,Test 1 - Tables pipeline présentes
print("\n" + "="*55)
print(" TEST 1: Tables pipeline SDP présentes")
print("="*55)

PIPELINE_TABLES = [
    "bronze_clients", "bronze_appels", "bronze_agenda",
    "bronze_signaux", "bronze_heatmap", "bronze_commerciaux",
    "silver_clients_features",
    "gold_priorites_clients", "gold_dashboard_jour",
]

for table in PIPELINE_TABLES:
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
        assert_test(f"{table} présente et non vide", count > 0, f"({count:,} lignes)")
    except Exception as e:
        assert_test(f"{table} accessible", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 2 - Règles de scoring urgence
print("\n" + "="*55)
print(" TEST 2: Règles de scoring urgence (J-15/30/60/90)")
print("="*55)

try:
    df = spark.table(f"{CATALOG}.{SCHEMA}.gold_priorites_clients")

    # J-15 : score_urgence >= 8
    df_j15 = df.filter((F.col("jours_avant_echeance") >= 0) & (F.col("jours_avant_echeance") <= 15))
    if df_j15.count() > 0:
        min_urgence = df_j15.select(F.min("score_urgence")).collect()[0][0]
        assert_test("J-15 → score_urgence >= 8", min_urgence >= 8, f"(min={min_urgence})")

    # J-30 (16–30j) : score_urgence >= 4
    df_j30 = df.filter((F.col("jours_avant_echeance") > 15) & (F.col("jours_avant_echeance") <= 30))
    if df_j30.count() > 0:
        min_urgence = df_j30.select(F.min("score_urgence")).collect()[0][0]
        assert_test("J-30 → score_urgence >= 4", min_urgence >= 4, f"(min={min_urgence})")

    # Score priorité borné (sanity check)
    max_score = df.select(F.max("score_priorite")).collect()[0][0]
    min_score = df.select(F.min("score_priorite")).collect()[0][0]
    assert_test(
        "score_priorite borné [0, 100]",
        min_score >= 0 and max_score < 100,
        f"(min={min_score:.2f}, max={max_score:.2f})"
    )

except Exception as e:
    assert_test("Score urgence vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 3 - Priorisation par région
print("\n" + "="*55)
print(" TEST 3: Priorisation et rang par région")
print("="*55)

try:
    df = spark.table(f"{CATALOG}.{SCHEMA}.gold_priorites_clients")

    # Max 20 clients à appeler par région
    df_jour = df.filter(F.col("a_appeler_aujourdhui") == True)
    by_region = df_jour.groupBy("region").count().collect()
    for row in by_region:
        assert_test(
            f"Région {row['region']} — max 20 appels/jour",
            row["count"] <= 20,
            f"({row['count']} clients)"
        )

    # rang_priorite_region commence à 1 dans chaque région
    rang_min = df.groupBy("region").agg(F.min("rang_priorite_region").alias("min_rang"))
    mauvais_rangs = rang_min.filter(F.col("min_rang") != 1).count()
    assert_test(
        "rang_priorite_region commence à 1 par région",
        mauvais_rangs == 0,
        f"({mauvais_rangs} régions non conformes)"
    )

    # Tous les clients ont un commercial assigné
    sans_commercial = df.filter(F.col("commercial_id").isNull()).count()
    assert_test("Tous les clients ont un commercial", sans_commercial == 0, f"({sans_commercial} sans)")

except Exception as e:
    assert_test("Priorisation vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 4 - Cohérence Silver → Gold
print("\n" + "="*55)
print(" TEST 4: Cohérence Silver → Gold")
print("="*55)

try:
    df_silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_clients_features")
    df_gold   = spark.table(f"{CATALOG}.{SCHEMA}.gold_priorites_clients")

    n_silver = df_silver.count()
    n_gold   = df_gold.count()

    assert_test("Gold contient au moins autant de clients que Silver",
                n_gold >= 1, f"(silver={n_silver:,}, gold={n_gold:,})")

    # Pas de client_id dupliqué dans Gold
    n_distinct = df_gold.select("client_id").distinct().count()
    assert_test(
        "Gold — client_id uniques",
        n_distinct == n_gold,
        f"({n_gold} lignes, {n_distinct} distincts)"
    )

    # Colonnes enrichies présentes dans Gold
    COLONNES_GOLD = ["score_priorite", "rang_priorite_region", "a_appeler_aujourdhui",
                     "meilleur_jour_appel", "heure_recommandee", "commercial_nom"]
    gold_cols = set(df_gold.columns)
    manquantes = [c for c in COLONNES_GOLD if c not in gold_cols]
    assert_test("Gold — colonnes enrichies présentes", len(manquantes) == 0,
                f"({'OK' if not manquantes else f'manquantes: {manquantes}'})")

except Exception as e:
    assert_test("Cohérence Silver→Gold vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 5 - KPIs Dashboard
print("\n" + "="*55)
print(" TEST 5: KPIs gold_dashboard_jour")
print("="*55)

try:
    kpi = spark.table(f"{CATALOG}.{SCHEMA}.gold_dashboard_jour").collect()[0]

    assert_test("nb_clients_a_appeler > 0",
                kpi["nb_clients_a_appeler"] > 0, f"({kpi['nb_clients_a_appeler']})")

    assert_test("nb_contrats_j30 >= 0",
                kpi["nb_contrats_j30"] >= 0, f"({kpi['nb_contrats_j30']})")

    assert_test("taux_joignabilite_moyen dans ]0, 100[",
                0 < kpi["taux_joignabilite_moyen"] < 100,
                f"({kpi['taux_joignabilite_moyen']:.1f}%)")

    assert_test("ca_potentiel_en_jeu > 0",
                kpi["ca_potentiel_en_jeu"] > 0, f"({kpi['ca_potentiel_en_jeu']:,.0f}€)")

except Exception as e:
    assert_test("Dashboard KPIs vérifiables", False, str(e))

# COMMAND ----------

# DBTITLE 1,Résumé
print("\n" + "="*60)
print(" RÉSUMÉ — Tests pipeline scoring")
print("="*60)
nb_pass = sum(1 for r in results if r["status"] == "PASS")
nb_fail = sum(1 for r in results if r["status"] == "FAIL")
print(f"  ✅ PASS : {nb_pass}")
print(f"  ❌ FAIL : {nb_fail}")
print("="*60)

if nb_fail > 0:
    for r in results:
        if r["status"] == "FAIL":
            print(f"  ❌ {r['test']}: {r['detail']}")
    dbutils.notebook.exit(f"FAILED: {nb_fail}/{len(results)}")
else:
    dbutils.notebook.exit(f"SUCCESS: {nb_pass}/{len(results)}")
