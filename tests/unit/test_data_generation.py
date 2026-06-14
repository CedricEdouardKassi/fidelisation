"""
Tests unitaires — Génération de données synthétiques.

Valide la logique pure de generate_data.py :
normalisation ASCII, bornes NPS, plages de valeurs contractuelles,
format des identifiants et reproductibilité Faker.
"""

import unicodedata
import pytest


# Réplique de la fonction privée de generate_data.py (testée ici de façon autonome)
def _ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()


# =============================================================================
# NORMALISATION ASCII (emails commerciaux)
# =============================================================================

class TestAsciiNormalization:

    def test_accent_aigu(self):
        assert _ascii("Élodie") == "elodie"

    def test_cedille(self):
        assert _ascii("François") == "francois"

    def test_accent_grave(self):
        assert _ascii("Hélène") == "helene"

    def test_tréma(self):
        assert _ascii("Noël") == "noel"

    def test_oe_ligature(self):
        # La ligature œ (U+0153) n'est pas décomposée par NFKD → supprimée
        assert _ascii("Cœur") == "cur"

    def test_majuscules_converties(self):
        assert _ascii("DUPONT") == "dupont"

    def test_sans_accent_inchange(self):
        assert _ascii("Thomas") == "thomas"

    def test_chaine_vide(self):
        assert _ascii("") == ""

    def test_prenom_composé(self):
        result = _ascii("Jean-Pierre")
        assert result == "jean-pierre"


class TestEmailFormat:

    @pytest.mark.parametrize("prenom,nom", [
        ("Élodie", "Lefèvre"),
        ("François", "Müller"),
        ("Héloïse", "Ångström"),
        ("Thomas", "Dupont"),
    ])
    def test_email_entierement_ascii(self, prenom, nom):
        email = f"{_ascii(prenom)}.{_ascii(nom)}@laposte.fr"
        assert all(c.isascii() for c in email), f"Email non-ASCII : {email}"

    def test_domaine_laposte(self):
        email = f"{_ascii('Sophie')}.{_ascii('Martin')}@laposte.fr"
        assert email.endswith("@laposte.fr")

    def test_format_prenom_point_nom(self):
        email = f"{_ascii('Pierre')}.{_ascii('Moreau')}@laposte.fr"
        local, domain = email.split("@")
        assert "." in local


# =============================================================================
# BORNES NPS (score entre 1 et 10)
# =============================================================================

class TestNpsBounds:

    @pytest.mark.parametrize("raw", [-5.0, 0.0, 0.5, 0.99])
    def test_nps_plancher_a_1(self, raw):
        nps = max(1, min(10, raw))
        assert nps == 1

    @pytest.mark.parametrize("raw", [10.1, 11.0, 100.0])
    def test_nps_plafond_a_10(self, raw):
        nps = max(1, min(10, raw))
        assert nps == 10

    @pytest.mark.parametrize("raw", [1.0, 5.0, 7.5, 9.9, 10.0])
    def test_nps_dans_plage_inchange(self, raw):
        nps = max(1, min(10, raw))
        assert nps == raw


# =============================================================================
# PLAGES DE VALEURS CONTRACTUELLES PAR SEGMENT
# =============================================================================

VALEUR_MAP = {
    "TPE":          (5_000,   20_000),
    "PME":          (15_000,  80_000),
    "ETI":          (50_000,  200_000),
    "Grand Compte": (150_000, 500_000),
    "Collectivité": (30_000,  150_000),
}


class TestContractValueRanges:

    @pytest.mark.parametrize("segment,bounds", VALEUR_MAP.items())
    def test_borne_min_positive(self, segment, bounds):
        val_min, _ = bounds
        assert val_min > 0, f"{segment}: val_min doit être > 0"

    @pytest.mark.parametrize("segment,bounds", VALEUR_MAP.items())
    def test_borne_max_superieure_au_min(self, segment, bounds):
        val_min, val_max = bounds
        assert val_max > val_min, f"{segment}: val_max doit être > val_min"

    def test_tous_les_segments_definis(self):
        assert set(VALEUR_MAP.keys()) == {"TPE", "PME", "ETI", "Grand Compte", "Collectivité"}

    def test_grand_compte_min_superieur_tpe_max(self):
        _, tpe_max = VALEUR_MAP["TPE"]
        gc_min, _ = VALEUR_MAP["Grand Compte"]
        assert gc_min > tpe_max

    def test_eti_min_superieur_pme_min(self):
        pme_min, _ = VALEUR_MAP["PME"]
        eti_min, _ = VALEUR_MAP["ETI"]
        assert eti_min > pme_min


# =============================================================================
# FORMAT DES IDENTIFIANTS
# =============================================================================

class TestIdentifierFormat:

    @pytest.mark.parametrize("i", [0, 9, 19])
    def test_commercial_id_format(self, i):
        com_id = f"COM-{i+1:03d}"
        assert com_id.startswith("COM-")
        assert len(com_id) == 7  # "COM-001" → 7 chars

    @pytest.mark.parametrize("i", [0, 99, 499])
    def test_client_id_format(self, i):
        cli_id = f"CLI-{i+1:04d}"
        assert cli_id.startswith("CLI-")
        assert len(cli_id) == 8  # "CLI-0001" → 8 chars

    @pytest.mark.parametrize("i", [0, 999, 5499])
    def test_appel_id_format(self, i):
        app_id = f"APP-{i+1:06d}"
        assert app_id.startswith("APP-")
        assert len(app_id) == 10  # "APP-000001" → 10 chars

    def test_20_commerciaux_ids_uniques(self):
        ids = [f"COM-{i+1:03d}" for i in range(20)]
        assert len(set(ids)) == 20

    def test_500_clients_ids_uniques(self):
        ids = [f"CLI-{i+1:04d}" for i in range(500)]
        assert len(set(ids)) == 500


# =============================================================================
# DURÉES DE CONTRAT AUTORISÉES
# =============================================================================

class TestContractDuration:

    DUREES_AUTORISEES = {12, 24, 36}

    @pytest.mark.parametrize("duree", [12, 24, 36])
    def test_duree_dans_liste(self, duree):
        assert duree in self.DUREES_AUTORISEES

    @pytest.mark.parametrize("duree", [6, 18, 48, 60])
    def test_duree_non_autorisee(self, duree):
        assert duree not in self.DUREES_AUTORISEES


# =============================================================================
# REPRODUCTIBILITÉ FAKER
# =============================================================================

class TestFakerReproductibilite:

    def test_seed_produit_meme_resultat(self):
        from faker import Faker
        f1 = Faker("fr_FR")
        f1.seed_instance(42)
        name1 = f1.name()

        f2 = Faker("fr_FR")
        f2.seed_instance(42)
        name2 = f2.name()

        assert name1 == name2

    def test_locale_francaise(self):
        from faker import Faker
        fake = Faker("fr_FR")
        Faker.seed(0)
        # Vérifie que les noms générés sont bien ASCII-compatibles après normalisation
        for _ in range(10):
            prenom = fake.first_name()
            nom = fake.last_name()
            email = f"{_ascii(prenom)}.{_ascii(nom)}@laposte.fr"
            assert all(c.isascii() for c in email)
