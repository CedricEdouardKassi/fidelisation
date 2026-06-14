# Databricks notebook source
# DBTITLE 1,Configuration
# =============================================================================
# TESTS D'INTÉGRATION — Intégrité des tables source
# Exécuter depuis le workspace Databricks après generate_data.py
# =============================================================================

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

# DBTITLE 1,Test 1 - Existence et volumétrie des tables source
print("\n" + "="*55)
print(" TEST 1: Existence et volumétrie des tables source")
print("="*55)

TABLES_MIN_ROWS = {
    "commerciaux":         20,
    "clients":             500,
    "historique_appels":   4_000,
    "agenda_commerciaux":  1_000,
    "signaux_contrats":    50,
    "heatmap_joignabilite": 40,
}

for table, min_rows in TABLES_MIN_ROWS.items():
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
        assert_test(
            f"{table} — volume suffisant",
            count >= min_rows,
            f"({count:,} lignes, attendu >= {min_rows:,})"
        )
    except Exception as e:
        assert_test(f"{table} — accessible", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 2 - Schémas des tables
print("\n" + "="*55)
print(" TEST 2: Colonnes obligatoires présentes")
print("="*55)

EXPECTED_COLUMNS = {
    "commerciaux":   ["commercial_id", "prenom", "nom", "email", "region"],
    "clients":       ["client_id", "raison_sociale", "segment", "commercial_id",
                      "valeur_contrat_annuel", "date_fin_contrat", "score_nps",
                      "canal_prefere", "nb_reclamations_12m"],
    "historique_appels": ["appel_id", "client_id", "commercial_id", "date_appel",
                          "resultat", "duree_secondes", "direction"],
    "signaux_contrats":  ["client_id", "type_signal", "score_signal_pts",
                          "jours_avant_echeance", "score_ml_propension"],
    "heatmap_joignabilite": ["jour_semaine", "heure_slot", "taux_reponse_moyen"],
}

for table, cols in EXPECTED_COLUMNS.items():
    try:
        df_cols = set(spark.table(f"{CATALOG}.{SCHEMA}.{table}").columns)
        missing = [c for c in cols if c not in df_cols]
        assert_test(
            f"{table} — colonnes présentes",
            len(missing) == 0,
            f"({f'manquantes: {missing}' if missing else 'OK'})"
        )
    except Exception as e:
        assert_test(f"{table} — schéma vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 3 - Intégrité référentielle
print("\n" + "="*55)
print(" TEST 3: Intégrité référentielle (FK)")
print("="*55)

from pyspark.sql import functions as F

# Tous les clients ont un commercial valide
try:
    df_clients    = spark.table(f"{CATALOG}.{SCHEMA}.clients")
    df_commerciaux = spark.table(f"{CATALOG}.{SCHEMA}.commerciaux")
    orphans = df_clients.join(df_commerciaux, "commercial_id", "left_anti").count()
    assert_test("clients → commercial_id valide", orphans == 0, f"({orphans} orphelins)")
except Exception as e:
    assert_test("FK clients.commercial_id", False, str(e))

# Tous les appels référencent un client existant
try:
    df_appels  = spark.table(f"{CATALOG}.{SCHEMA}.historique_appels")
    df_clients = spark.table(f"{CATALOG}.{SCHEMA}.clients")
    orphans_appels = df_appels.join(df_clients, "client_id", "left_anti").count()
    assert_test("appels → client_id valide", orphans_appels == 0, f"({orphans_appels} orphelins)")
except Exception as e:
    assert_test("FK appels.client_id", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 4 - Qualité des données
print("\n" + "="*55)
print(" TEST 4: Qualité des données")
print("="*55)

try:
    df_clients = spark.table(f"{CATALOG}.{SCHEMA}.clients")

    # NPS dans [1, 10]
    nps_hors_range = df_clients.filter((F.col("score_nps") < 1) | (F.col("score_nps") > 10)).count()
    assert_test("NPS borné entre 1 et 10", nps_hors_range == 0, f"({nps_hors_range} hors plage)")

    # Valeur contrat positive
    ca_negatif = df_clients.filter(F.col("valeur_contrat_annuel") <= 0).count()
    assert_test("valeur_contrat_annuel > 0", ca_negatif == 0, f"({ca_negatif} valeurs invalides)")

    # Segments valides
    SEGMENTS_VALIDES = {"TPE", "PME", "ETI", "Grand Compte", "Collectivité"}
    segments_inconnus = df_clients.filter(~F.col("segment").isin(*SEGMENTS_VALIDES)).count()
    assert_test("Segments tous valides", segments_inconnus == 0, f"({segments_inconnus} inconnus)")

    # Date fin > date début
    dates_invalides = df_clients.filter(F.col("date_fin_contrat") <= F.col("date_contrat")).count()
    assert_test("date_fin_contrat > date_contrat", dates_invalides == 0, f"({dates_invalides} invalides)")

except Exception as e:
    assert_test("Qualité données clients", False, str(e))

try:
    df_appels = spark.table(f"{CATALOG}.{SCHEMA}.historique_appels")
    duree_invalide = df_appels.filter(F.col("duree_secondes") < 0).count()
    assert_test("duree_secondes >= 0", duree_invalide == 0, f"({duree_invalide} invalides)")
except Exception as e:
    assert_test("Qualité données appels", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 5 - Heatmap complète
print("\n" + "="*55)
print(" TEST 5: Heatmap joignabilité complète")
print("="*55)

try:
    df_heatmap = spark.table(f"{CATALOG}.{SCHEMA}.heatmap_joignabilite")
    count = df_heatmap.count()
    assert_test("Heatmap — 40 créneaux (5j × 8h)", count == 40, f"({count} lignes)")

    taux_invalide = df_heatmap.filter(
        (F.col("taux_reponse_moyen") < 0) | (F.col("taux_reponse_moyen") > 1)
    ).count()
    assert_test("taux_reponse_moyen dans [0,1]", taux_invalide == 0, f"({taux_invalide} invalides)")
except Exception as e:
    assert_test("Heatmap vérifiable", False, str(e))

# COMMAND ----------

# DBTITLE 1,Résumé
print("\n" + "="*60)
print(" RÉSUMÉ — Tests intégrité tables source")
print("="*60)
nb_pass = sum(1 for r in results if r["status"] == "PASS")
nb_fail = sum(1 for r in results if r["status"] == "FAIL")
print(f"  ✅ PASS : {nb_pass}")
print(f"  ❌ FAIL : {nb_fail}")
print("="*60)

if nb_fail > 0:
    print("\nÉCHECS :")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  - {r['test']}: {r['detail']}")
    dbutils.notebook.exit(f"FAILED: {nb_fail}/{len(results)}")
else:
    dbutils.notebook.exit(f"SUCCESS: {nb_pass}/{len(results)}")
