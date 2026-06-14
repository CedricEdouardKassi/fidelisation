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

# DBTITLE 1,Test 1 - Tables pipeline présentes et non vides
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
        assert_test(f"{table} — présente et non vide", count > 0, f"({count:,} lignes)")
    except Exception as e:
        assert_test(f"{table} — accessible", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 2 - Colonnes Silver
print("\n" + "="*55)
print(" TEST 2: Colonnes de silver_clients_features")
print("="*55)

COLONNES_SILVER = [
    # Depuis clients
    "client_id", "segment", "valeur_contrat_annuel", "potentiel_upsell",
    "score_nps", "nb_reclamations_12m", "canal_prefere", "renouvellement_auto",
    "jours_avant_echeance",
    # Agrégats appels
    "nb_appels_12m", "nb_decroches_12m", "nb_appels_sortants_12m",
    "taux_reponse_historique", "jours_depuis_dernier_contact",
    # Signaux (encodés en entiers 0/1)
    "malus_auto_off", "offre_trimestrielle_active", "campagne_en_cours", "saisonnalite_detectee",
    "score_signal_pts", "score_ml_propension",
    # Scores partiels
    "score_urgence", "score_joignabilite", "score_valeur", "score_signal_marketing",
]

try:
    silver_cols = set(spark.table(f"{CATALOG}.{SCHEMA}.silver_clients_features").columns)
    manquantes = [c for c in COLONNES_SILVER if c not in silver_cols]
    assert_test("silver — toutes les colonnes présentes", len(manquantes) == 0,
                f"({'OK' if not manquantes else f'manquantes: {manquantes}'})")
except Exception as e:
    assert_test("silver — schéma vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 3 - Règles de scoring urgence dans Silver
print("\n" + "="*55)
print(" TEST 3: Règles de score_urgence dans Silver")
print("="*55)

try:
    df = spark.table(f"{CATALOG}.{SCHEMA}.silver_clients_features")

    # J-15 avec renouvellement_auto=True → score_urgence == 8
    df_j15_auto = df.filter(
        (F.col("jours_avant_echeance").between(0, 15)) & F.col("renouvellement_auto")
    )
    if df_j15_auto.count() > 0:
        min_u = df_j15_auto.select(F.min("score_urgence")).collect()[0][0]
        assert_test("J-15 + auto=True → score_urgence == 8", min_u == 8, f"(min={min_u})")

    # J-15 avec malus_auto_off=1 → score_urgence == 16
    df_j15_noauto = df.filter(
        (F.col("jours_avant_echeance").between(0, 15)) & (F.col("malus_auto_off") == 1)
    )
    if df_j15_noauto.count() > 0:
        min_u = df_j15_noauto.select(F.min("score_urgence")).collect()[0][0]
        assert_test("J-15 + malus_auto_off=1 → score_urgence == 16", min_u == 16, f"(min={min_u})")

    # Au-delà de 90j → score_urgence == 0
    df_hors_fenetre = df.filter(F.col("jours_avant_echeance") > 90)
    if df_hors_fenetre.count() > 0:
        max_u = df_hors_fenetre.select(F.max("score_urgence")).collect()[0][0]
        assert_test("> 90 jours → score_urgence == 0", max_u == 0, f"(max={max_u})")

    # jours_avant_echeance calculé en temps réel (pas de valeur aberrante)
    null_jae = df.filter(F.col("jours_avant_echeance").isNull()).count()
    assert_test("jours_avant_echeance non nul pour tous", null_jae == 0, f"({null_jae} nulls)")

except Exception as e:
    assert_test("Score urgence silver vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 4 - Colonnes Gold et cohérence scores
print("\n" + "="*55)
print(" TEST 4: Colonnes Gold et cohérence score_priorite")
print("="*55)

COLONNES_GOLD = [
    "score_priorite", "rang_priorite_region", "a_appeler_aujourdhui",
    "meilleur_jour_appel", "heure_recommandee", "commercial_nom",
]

try:
    df = spark.table(f"{CATALOG}.{SCHEMA}.gold_priorites_clients")

    gold_cols = set(df.columns)
    manquantes = [c for c in COLONNES_GOLD if c not in gold_cols]
    assert_test("gold — colonnes enrichies présentes", len(manquantes) == 0,
                f"({'OK' if not manquantes else f'manquantes: {manquantes}'})")

    # score_priorite dans [0, 50] (bornes raisonnables)
    max_s = df.select(F.max("score_priorite")).collect()[0][0]
    min_s = df.select(F.min("score_priorite")).collect()[0][0]
    assert_test("score_priorite borné [0, 50]",
                min_s >= 0 and max_s < 50, f"(min={min_s:.4f}, max={max_s:.4f})")

    # rang_priorite_region commence à 1 dans chaque région
    rang_min = df.groupBy("region").agg(F.min("rang_priorite_region").alias("min_rang"))
    mauvais = rang_min.filter(F.col("min_rang") != 1).count()
    assert_test("rang_priorite_region commence à 1 par région",
                mauvais == 0, f"({mauvais} régions non conformes)")

    # client_id uniques dans gold
    n_total  = df.count()
    n_unique = df.select("client_id").distinct().count()
    assert_test("gold — client_id uniques", n_total == n_unique,
                f"({n_total} lignes, {n_unique} distincts)")

except Exception as e:
    assert_test("Colonnes gold vérifiables", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 5 - Priorisation quotidienne
print("\n" + "="*55)
print(" TEST 5: Flag a_appeler_aujourdhui et priorisation")
print("="*55)

try:
    df = spark.table(f"{CATALOG}.{SCHEMA}.gold_priorites_clients")

    # a_appeler_aujourdhui correspond bien à rang <= 20
    # (RANK peut créer des ex-æquo → tolérance de quelques lignes supplémentaires)
    df_jour = df.filter(F.col("a_appeler_aujourdhui") == True)
    by_region = df_jour.groupBy("region").count().collect()
    for row in by_region:
        assert_test(
            f"Région {row['region']} — nombre d'appels raisonnable (≤ 25)",
            row["count"] <= 25,
            f"({row['count']} clients — RANK peut créer des ex-æquo au seuil 20)",
        )

    # Les clients avec rang 1 ont le score_priorite le plus élevé de leur région
    df_rang1 = df.filter(F.col("rang_priorite_region") == 1)
    df_max   = df.groupBy("region").agg(F.max("score_priorite").alias("max_score"))
    ecarts   = (
        df_rang1
        .join(df_max, "region")
        .filter(F.round(F.col("score_priorite"), 2) < F.round(F.col("max_score"), 2))
        .count()
    )
    assert_test("rang 1 = meilleur score par région", ecarts == 0, f"({ecarts} incohérences)")

    # Tous les clients ont un commercial assigné
    sans_com = df.filter(F.col("commercial_id").isNull()).count()
    assert_test("Tous les clients ont un commercial", sans_com == 0, f"({sans_com} sans)")

except Exception as e:
    assert_test("Priorisation vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 6 - KPIs Dashboard
print("\n" + "="*55)
print(" TEST 6: KPIs gold_dashboard_jour")
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
print(" RÉSUMÉ — Tests pipeline scoring Silver + Gold")
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
