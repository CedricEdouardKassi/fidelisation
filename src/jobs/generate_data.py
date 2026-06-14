# Databricks notebook source
# DBTITLE 1,Installation des dépendances
# MAGIC %pip install faker

# COMMAND ----------

# DBTITLE 1,Configuration et imports
# =============================================================================
# MOTEUR DE FIDÉLISATION INTELLIGENT — Génération de Données Synthétiques
# =============================================================================
# Ce notebook génère des jeux de données réalistes pour la démo:
# - 500 clients B2B avec contrats
# - 20 commerciaux avec agendas
# - 5000+ appels historiques (12 mois)
# - Signaux contrats (échéances, NPS, réclamations)
# - Heatmap de joignabilité
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta, date
import unicodedata
import numpy as np
from faker import Faker

fake = Faker("fr_FR")
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Paramètres injectés par le job (ou valeurs par défaut)
dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "fidelisation")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"✅ Cible: {CATALOG}.{SCHEMA}")


def _ascii(s):
    """Normalise un nom pour construire un identifiant email sans accents."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()


# COMMAND ----------

# DBTITLE 1,Génération des Commerciaux
# =============================================================================
# TABLE: commerciaux (20 commerciaux)
# =============================================================================
regions = ["IDF", "IDF", "IDF", "IDF", "IDF", "Lyon", "Lyon", "Lyon",
           "Marseille", "Marseille", "Toulouse", "Toulouse", "Bordeaux", "Bordeaux",
           "Nantes", "Nantes", "Lille", "Lille", "Strasbourg", "Strasbourg"]

commerciaux_data = []
for i in range(20):
    prenom = fake.first_name()
    nom = fake.last_name()
    commerciaux_data.append({
        "commercial_id": f"COM-{i+1:03d}",
        "prenom": prenom,
        "nom": nom,
        "email": f"{_ascii(prenom)}.{_ascii(nom)}@laposte.fr",
        "region": regions[i],
        "nb_clients_portefeuille": random.randint(15, 35),
        "objectif_renouvellement_mensuel": random.randint(8, 15),
        "date_embauche": str(date(2020, 1, 1) + timedelta(days=random.randint(0, 1500)))
    })

df_commerciaux = spark.createDataFrame(commerciaux_data)
df_commerciaux.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.commerciaux")
print(f"✅ commerciaux: {df_commerciaux.count()} lignes")
df_commerciaux.show(5)

# COMMAND ----------

# DBTITLE 1,Génération du Portefeuille Clients
# =============================================================================
# TABLE: clients (500 clients B2B)
# =============================================================================
segments = ["PME", "ETI", "Grand Compte", "TPE", "Collectivité"]
offres = ["Courrier Pro", "Colis Express", "Marketing Direct", "Logistique Intégrée",
          "GED Numérique", "Affranchissement Connecté"]

clients_data = []
for i in range(500):
    segment = random.choice(segments)
    valeur_map = {"TPE": (5000, 20000), "PME": (15000, 80000), "ETI": (50000, 200000),
                  "Grand Compte": (150000, 500000), "Collectivité": (30000, 150000)}
    val_min, val_max = valeur_map[segment]
    valeur_contrat = round(random.uniform(val_min, val_max), 2)

    date_contrat = date(2023, 1, 1) + timedelta(days=random.randint(0, 900))
    duree_contrat_mois = random.choice([12, 24, 36])
    date_fin = date_contrat + timedelta(days=duree_contrat_mois * 30)

    nps = round(random.gauss(7, 2), 1)
    nps = max(1, min(10, nps))

    commercial_idx = i % 20

    clients_data.append({
        "client_id": f"CLI-{i+1:04d}",
        "raison_sociale": fake.company(),
        "segment": segment,
        "ville": fake.city(),
        "commercial_id": f"COM-{commercial_idx+1:03d}",
        "offre_actuelle": random.choice(offres),
        "valeur_contrat_annuel": valeur_contrat,
        "date_contrat": str(date_contrat),
        "date_fin_contrat": str(date_fin),
        "duree_contrat_mois": duree_contrat_mois,
        "renouvellement_auto": random.choice([True, True, False]),
        "score_nps": nps,
        "anciennete_mois": random.randint(6, 120),
        "nb_reclamations_12m": random.choices([0, 0, 0, 1, 1, 2, 3], k=1)[0],
        "potentiel_upsell": round(random.uniform(0, valeur_contrat * 0.4), 2),
        "canal_prefere": random.choice(["telephone", "telephone", "telephone", "email", "email", "sms"]),
        "score_initial_manuel": random.randint(1, 5)
    })

df_clients = spark.createDataFrame(clients_data)
df_clients.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.clients")
print(f"✅ clients: {df_clients.count()} lignes")
df_clients.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Génération Historique des Appels
# =============================================================================
# TABLE: historique_appels (5000+ appels sur 12 mois)
# =============================================================================
resultats = ["décroché", "décroché", "décroché", "absent", "absent", "messagerie", "occupé"]
canaux = ["telephone", "telephone", "telephone", "email", "sms"]
notes_templates = [
    "Client satisfait, à recontacter en fin de mois",
    "Demande info offre colis express",
    "Intéressé par upgrade marketing direct",
    "Absent - rappeler demain matin",
    "Réclamation livraison en cours",
    "Confirmé renouvellement",
    "Hésite entre nous et concurrent",
    "Besoin devis logistique intégrée",
    "Très occupé, rappeler semaine pro",
    "Contact positif, envoi proposition commerciale",
    "",
    "RAS - suivi de routine",
    "Client en congés jusqu'au 15"
]

appels_data = []
base_date = datetime(2025, 6, 11)

for i in range(5500):
    days_ago = int(np.random.exponential(90))
    days_ago = min(days_ago, 365)
    call_date = base_date - timedelta(days=days_ago)

    hour_weights = [0.05, 0.15, 0.20, 0.15, 0.05, 0.10, 0.15, 0.10, 0.05]
    hours = [9, 10, 11, 12, 14, 15, 16, 17, 18]
    hour = random.choices(hours, weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    call_datetime = call_date.replace(hour=hour, minute=minute)

    if call_datetime.weekday() >= 5:
        continue

    resultat = random.choice(resultats)
    duree = 0 if resultat in ["absent", "occupé"] else random.randint(30, 900)
    direction = random.choices(["sortant", "sortant", "sortant", "entrant"], k=1)[0]

    appels_data.append({
        "appel_id": f"APP-{i+1:06d}",
        "client_id": f"CLI-{random.randint(1, 500):04d}",
        "commercial_id": f"COM-{random.randint(1, 20):03d}",
        "date_appel": str(call_datetime.date()),
        "heure_appel": f"{hour:02d}:{minute:02d}",
        "heure_slot": hour,
        "jour_semaine": ["Lun", "Mar", "Mer", "Jeu", "Ven"][call_datetime.weekday()],
        "direction": direction,
        "resultat": resultat,
        "duree_secondes": duree,
        "canal": random.choice(canaux) if direction == "sortant" else "telephone",
        "notes_commerciales": random.choice(notes_templates) if resultat == "décroché" else ""
    })

df_appels = spark.createDataFrame(appels_data)
df_appels.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.historique_appels")
print(f"✅ historique_appels: {df_appels.count()} lignes")
df_appels.show(5)

# COMMAND ----------

# DBTITLE 1,Génération Agenda Commerciaux
# =============================================================================
# TABLE: agenda_commerciaux (disponibilités)
# =============================================================================
agenda_data = []
today = date(2025, 6, 11)

for com_id in range(1, 21):
    for day_offset in range(30):
        jour = today + timedelta(days=day_offset)
        if jour.weekday() >= 5:
            continue

        jour_semaine = ["Lun", "Mar", "Mer", "Jeu", "Ven"][jour.weekday()]
        is_jour_entrant = random.random() < 0.3

        for slot in [9, 10, 11, 14, 15, 16, 17]:
            if is_jour_entrant:
                disponible = False
                motif = "appels_entrants"
            elif random.random() < 0.15:
                disponible = False
                motif = random.choice(["reunion", "formation", "conge", "rdv_client"])
            else:
                disponible = True
                motif = ""

            agenda_data.append({
                "commercial_id": f"COM-{com_id:03d}",
                "date_slot": str(jour),
                "jour_semaine": jour_semaine,
                "heure_slot": slot,
                "disponible": disponible,
                "motif_indisponibilite": motif
            })

df_agenda = spark.createDataFrame(agenda_data)
df_agenda.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.agenda_commerciaux")
print(f"✅ agenda_commerciaux: {df_agenda.count()} lignes")
df_agenda.show(5)

# COMMAND ----------

# DBTITLE 1,Génération Signaux Contrats
# =============================================================================
# TABLE: signaux_contrats (signaux déclencheurs pour la fidélisation)
# =============================================================================
signaux_data = []
today = date(2025, 6, 11)

clients_list = spark.table(f"{CATALOG}.{SCHEMA}.clients").collect()

for client in clients_list:
    client_id = client["client_id"]
    date_fin = datetime.strptime(client["date_fin_contrat"], "%Y-%m-%d").date()
    jours_avant_fin = (date_fin - today).days

    if 0 <= jours_avant_fin <= 90:
        if jours_avant_fin <= 15:
            urgence = "J-15"
            score_urgence_pts = 8
        elif jours_avant_fin <= 30:
            urgence = "J-30"
            score_urgence_pts = 4
        elif jours_avant_fin <= 60:
            urgence = "J-60"
            score_urgence_pts = 2
        else:
            urgence = "J-90"
            score_urgence_pts = 1

        signaux_data.append({
            "client_id": client_id,
            "type_signal": "echeance_contrat",
            "valeur_signal": urgence,
            "score_signal_pts": score_urgence_pts,
            "date_signal": str(today),
            "jours_avant_echeance": jours_avant_fin,
            "renouvellement_auto": client["renouvellement_auto"],
            "offre_trimestrielle_active": random.choice([True, False]),
            "campagne_en_cours": random.choice([True, False, False, False]),
            "saisonnalite_detectee": random.choice([True, False, False]),
            "score_ml_propension": round(random.uniform(0.2, 0.95), 3)
        })

    if client["score_nps"] < 5:
        signaux_data.append({
            "client_id": client_id,
            "type_signal": "nps_bas",
            "valeur_signal": f"NPS={client['score_nps']}",
            "score_signal_pts": 3,
            "date_signal": str(today),
            "jours_avant_echeance": jours_avant_fin if 0 <= jours_avant_fin <= 90 else -1,
            "renouvellement_auto": client["renouvellement_auto"],
            "offre_trimestrielle_active": False,
            "campagne_en_cours": False,
            "saisonnalite_detectee": False,
            "score_ml_propension": round(random.uniform(0.1, 0.5), 3)
        })

    if jours_avant_fin > 90 and random.random() < 0.15:
        signaux_data.append({
            "client_id": client_id,
            "type_signal": "silence_client",
            "valeur_signal": "sans_contact_90j",
            "score_signal_pts": 2,
            "date_signal": str(today),
            "jours_avant_echeance": jours_avant_fin,
            "renouvellement_auto": client["renouvellement_auto"],
            "offre_trimestrielle_active": random.choice([True, False]),
            "campagne_en_cours": False,
            "saisonnalite_detectee": False,
            "score_ml_propension": round(random.uniform(0.3, 0.7), 3)
        })

df_signaux = spark.createDataFrame(signaux_data)
df_signaux.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.signaux_contrats")
print(f"✅ signaux_contrats: {df_signaux.count()} lignes")
df_signaux.show(5)

# COMMAND ----------

# DBTITLE 1,Génération Heatmap Joignabilité
# =============================================================================
# TABLE: heatmap_joignabilite (taux réponse historique par créneau)
# =============================================================================
heatmap_ref = {
    "Lun": {9: 0.10, 10: 0.30, 11: 0.65, 14: 0.70, 15: 0.25, 16: 0.80, 17: 0.60, 18: 0.20},
    "Mar": {9: 0.15, 10: 0.45, 11: 0.75, 14: 0.60, 15: 0.30, 16: 0.85, 17: 0.70, 18: 0.25},
    "Mer": {9: 0.20, 10: 0.40, 11: 0.70, 14: 0.65, 15: 0.20, 16: 0.75, 17: 0.65, 18: 0.15},
    "Jeu": {9: 0.12, 10: 0.35, 11: 0.60, 14: 0.70, 15: 0.35, 16: 0.90, 17: 0.75, 18: 0.30},
    "Ven": {9: 0.08, 10: 0.20, 11: 0.45, 14: 0.55, 15: 0.15, 16: 0.50, 17: 0.40, 18: 0.10}
}

heatmap_data = []
for jour, slots in heatmap_ref.items():
    for heure, taux in slots.items():
        heatmap_data.append({
            "jour_semaine": jour,
            "heure_slot": heure,
            "taux_reponse_moyen": round(taux + random.uniform(-0.03, 0.03), 3),
            "nb_appels_historique": random.randint(80, 300),
            "nb_reponses": int(random.randint(80, 300) * taux)
        })

df_heatmap = spark.createDataFrame(heatmap_data)
df_heatmap.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.heatmap_joignabilite")
print(f"✅ heatmap_joignabilite: {df_heatmap.count()} lignes")
df_heatmap.show()

# COMMAND ----------

# DBTITLE 1,Résumé de la génération
# =============================================================================
# RÉSUMÉ — Tables générées
# =============================================================================
print("\n" + "="*60)
print(" MOTEUR DE FIDÉLISATION — Données générées avec succès")
print("="*60)
tables = ["commerciaux", "clients", "historique_appels", "agenda_commerciaux",
          "signaux_contrats", "heatmap_joignabilite"]
for t in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  ✅ {CATALOG}.{SCHEMA}.{t}: {count:,} lignes")
print("="*60)
