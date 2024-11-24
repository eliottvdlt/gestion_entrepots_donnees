import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

# Charger les variables d'environnement
load_dotenv()

# Configuration des paramètres
MONGO_URI_METEO = os.getenv("MONGODB_URL")
API_URL_METEO = os.getenv("API_URL_METEO_2")
METEO_API_KEY = os.getenv("METEO_API_KEY")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def fetch_and_store_meteo_data():
    """
    Récupère les données météo depuis l'API et les insère dans MongoDB.
    """
    try:
        # Connexion à MongoDB
        client = MongoClient(MONGO_URI_METEO)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Requête API
        headers = {"Authorization": f"Bearer {METEO_API_KEY}"}
        print(f"Requête envoyée à l'API météo : {API_URL_METEO}")
        response = requests.get(API_URL_METEO, headers=headers)

        if response.status_code == 200:
            content_json = response.json()

            # Récupération des sections de données nécessaires
            metadata = content_json.get('metadata', {})
            hourly = content_json.get('hourly', {})
            params = content_json.get('_params', [])

            # Afficher les sections disponibles
            print("Metadata disponible :", metadata)
            print("Liste des paramètres disponibles :", params)
            
            # Traitement des données horaires
            all_records = []
            for station_id, hourly_data in hourly.items():
                # Exclure les clés qui ne sont pas des stations (comme "_params")
                if station_id == "_params":
                    continue

                print(f"Traitement des données horaires pour la station {station_id}...")
                
                # Ajout de la station ID aux enregistrements
                for record in hourly_data:
                    record['station_id'] = station_id
                    all_records.append(record)

            # Créer un DataFrame Pandas
            df = pd.DataFrame(all_records)
            print("Aperçu du DataFrame :", df.head())

            # Vérifier si le DataFrame est vide
            if df.empty:
                print("Le DataFrame est vide. Aucun document à insérer.")
                return

            # Ajouter les métadonnées aux documents avant l'insertion
            for record in all_records:
                record['metadata'] = metadata

            # Insérer les données dans MongoDB
            collection.insert_many(all_records)
            print(f"Succès : {len(all_records)} documents insérés dans MongoDB.")

        else:
            print(f"Erreur API ({response.status_code}): {response.text}")

    except Exception as e:
        print(f"Erreur lors de l'exécution : {e}")

    finally:
        # Fermer la connexion MongoDB
        client.close()
        print("Connexion à MongoDB fermée.")

if __name__ == "__main__":
    fetch_and_store_meteo_data()
