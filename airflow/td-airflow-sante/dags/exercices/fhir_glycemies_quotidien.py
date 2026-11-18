"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path("/opt/airflow/donnees")
BASE = os.environ.get("FHIR_BASE", "https://hapi.fhir.org/baseR4")   # panne volontaire (TD.md, 4.4) : mettre une mauvaise URL
#BASE = "https://hapi.fhir.org/baseR5x"
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        #print("PARAMÈTRE SOURCE REÇU :", params["source"])

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []
        # À COMPLÉTER (étape 1) : boucle de pagination de la séance 1, qui remplit `ressources`
        #   - r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
        #   - r.raise_for_status() : une erreur HTTP fait échouer la tâche, donc déclenche un retry
        #   - ajouter les ressources de bundle["entry"], suivre le lien « next », puis requete = None

        # Parcourir toutes les pages de l'API FHIR
        while url:
            r = requests.get(
            url,
            params=requete,
            headers={"Accept": "application/fhir+json"},
            timeout=30
            )

        # Déclencher une erreur si la requête HTTP échoue
            r.raise_for_status()

            bundle = r.json()

            # Récupérer les observations de la page
            for entry in bundle.get("entry", []):
                if "resource" in entry:
                    ressources.append(entry["resource"])

        # Chercher le lien vers la page suivante
            url_suivante = None

            for lien in bundle.get("link", []):
                if lien.get("relation") == "next":
                    url_suivante = lien.get("url")
                    break
            url = url_suivante
            requete = None
        #raise NotImplementedError("Étape 1 à compléter (ou déclencher avec source = fichier)")
        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données

    @task
    def aplatir(chemin: str) -> str:
        """Étape 2 : une ligne par Observation (id, patient, date, statut, valeur, unite), en Parquet."""
        import pandas as pd

        # À COMPLÉTER (étape 2) : lire le NDJSON ligne par ligne (json.loads), construire un DataFrame,
        # l'écrire avec df.to_parquet(sortie, index=False) et renvoyer str(sortie)

        observations = []

        #Lecture fichier JSON ligne par ligne
        with open(chemin, "r", encoding="utf-8") as f:
            for ligne in f:
                if ligne.strip():
                    observation = json.loads(ligne)

                # Extraire les informations FHIR
                    quantite = observation.get("valueQuantity") or {}
                    sujet = observation.get("subject") or {}

                    observations.append({
                    "id": observation.get("id"),
                    "patient": sujet.get("reference"),
                    "date": observation.get("effectiveDateTime"),
                    "statut": observation.get("status"),
                    "valeur": quantite.get("value"),
                    "unite": quantite.get("unit")
                })
        # Transformer en DataFrame
        df = pd.DataFrame(
        observations,
        columns=["id", "patient", "date", "statut", "valeur", "unite"]
        )

    # Définir le chemin du fichier Parquet
        sortie = DONNEES / "transforme" / "glycemies.parquet"
        sortie.parent.mkdir(parents=True, exist_ok=True)

    # Enregistrer en Parquet
        df.to_parquet(sortie, index=False)

        print(f"{len(df)} lignes aplaties")
        print(f"Unités : {df['unite'].value_counts(dropna=False).to_dict()}")

        return str(sortie)

        #raise NotImplementedError("Étape 2 à compléter")

    @task
    def valider(chemin: str) -> str:
        """Étape 3 : schéma Pandera ; que faire des lignes invalides ?"""
        import pandas as pd
        import pandera.pandas as pa

        # À COMPLÉTER (étape 3) : schéma avec unite dans UNITES, valeur > 0, patient non nul ;
        # écrire les lignes valides en Parquet et renvoyer le chemin
        # Lire le fichier Parquet
        df = pd.read_parquet(chemin)

    # Définir le schéma de validation
        schema = pa.DataFrameSchema({
        "unite": pa.Column(
            str, pa.Check.isin(UNITES), nullable=False
        ),
        "valeur": pa.Column(
            float, pa.Check.gt(0), nullable=False,
            coerce=True
        ),
        "patient": pa.Column(
            str, nullable=False
        )
    })

    # Identifier les lignes valides
        masque = (
        df["unite"].isin(UNITES)
        & (pd.to_numeric(df["valeur"], errors="coerce") > 0)
        & df["patient"].notna()
        & (df["patient"].astype("string").str.strip() != "")
        ).fillna(False)

        df_valides = df.loc[masque].copy()
        df_rejetees = df.loc[~masque].copy()

    # Vérifier les lignes conservées avec Pandera
        df_valides = schema.validate(df_valides)

    # Enregistrer les données valides
        sortie = DONNEES / "valide" / "glycemies_valides.parquet"
        sortie.parent.mkdir(parents=True, exist_ok=True)

        df_valides.to_parquet(sortie, index=False)

        print(f"Lignes valides : {len(df_valides)}")
        print(f"Lignes rejetées : {len(df_rejetees)}")

        return str(sortie)
        #raise NotImplementedError("Étape 3 à compléter")

    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Étape 4 : DELETE + INSERT du jour dans donnees/entrepot.duckdb, donc rejouable sans doublon."""
        import duckdb

        # Déterminer le jour traité
        date_jour = jour(data_interval_start)

    # Chemin de la base DuckDB
        base = DONNEES / "entrepot.duckdb"

    # Connexion à DuckDB
        con = duckdb.connect(str(base))

        try:
        # Créer la table si elle n'existe pas
            con.execute("""
            CREATE TABLE IF NOT EXISTS glycemies (
                id VARCHAR,
                patient VARCHAR,
                date VARCHAR,
                statut VARCHAR,
                valeur_mg_dl DOUBLE,
                jour_chargement DATE
                )
            """)

        # Transaction : DELETE + INSERT
            con.execute("BEGIN TRANSACTION")

        # Supprimer les données du jour
            con.execute(
            "DELETE FROM glycemies WHERE jour_chargement = ?",
            [date_jour]
            )

        # Charger le Parquet et convertir les unités
            con.execute("""
            INSERT INTO glycemies
            SELECT
                id,
                patient,
                date,
                statut,
                CASE
                    WHEN unite = 'mg/dL' THEN valeur
                    WHEN unite = 'g/L' THEN valeur * 100
                    WHEN unite = 'mmol/L' THEN valeur * 18.016
                END AS valeur_mg_dl,
                CAST(? AS DATE) AS jour_chargement
            FROM read_parquet(?)
        """, [date_jour, chemin])

        # Valider la transaction
            con.execute("COMMIT")

        # Vérifier le nombre de lignes
            nb_jour = con.execute("""
            SELECT COUNT(*) FROM glycemies
            WHERE jour_chargement = ?
        """, [date_jour]).fetchone()[0]

            total = con.execute("""
            SELECT COUNT(*) FROM glycemies
        """).fetchone()[0]

            print(f"Glycémies chargées : {nb_jour}")
            print(f"Total dans DuckDB : {total}")

        except Exception:
            con.execute("ROLLBACK")
            raise

        finally:
            con.close()


        # À COMPLÉTER (étape 4) : table glycemies (valeur convertie en mg/dL comme en séance 1),
        # suppression des lignes du jour puis insertion depuis read_parquet(chemin)
        #raise NotImplementedError("Étape 4 à compléter")

    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()