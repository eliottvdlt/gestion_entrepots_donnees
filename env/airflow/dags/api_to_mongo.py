import requests
from dotenv import load_dotenv
import os
from pymongo import MongoClient

# Charger les variables d'environnement
load_dotenv()

# Récupérer les paramètres de l'API et MongoDB
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DBNAME = os.getenv("MONGO_DBNAME")


class AviationAPIClient:
    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    def fetch_data(self, flight_date, offset=0):
        """
        Récupère les données de l'API AviationStack pour une date spécifique.
        """
        try:
            params = {
                "access_key": self.api_key,
                "flight_date": flight_date,
                "offset": offset,
                "dep_iata": "RNS"  # Filtrer à la source
            }
            print(f"Requête envoyée : {self.api_url} avec paramètres {params}")
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()

            data = response.json()
            records = data.get("data", [])
            print(f"{len(records)} enregistrements récupérés.")
            return records
        except Exception as e:
            print(f"Erreur lors de la récupération des données : {e}")
            return []


class MongoDBPipeline:
    def __init__(self, mongo_uri, dbname):
        self.mongo_uri = mongo_uri
        self.dbname = dbname

        # Connexion à MongoDB
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.dbname]
        self.collection = self.db["flights"]

        # Réinitialiser la collection
        self.collection.drop()
        print("Collection MongoDB supprimée et réinitialisée.")

    def insert_data_to_mongodb(self, records):
        """
        Insère uniquement les enregistrements correspondant à dep_iata='RNS' dans MongoDB.
        """
        try:
            # Filtrer localement les enregistrements
            filtered_records = [
                record for record in records
                if record.get("departure", {}).get("iata") == "RNS"
            ]

            if filtered_records:
                result = self.collection.insert_many(filtered_records)
                print(f"{len(result.inserted_ids)} documents insérés dans MongoDB.")
            else:
                print("Aucune donnée correspondant à dep_iata='RNS' à insérer.")
        except Exception as e:
            print(f"Erreur lors de l'insertion des données dans MongoDB : {e}")

    def close_connection(self):
        """
        Ferme la connexion MongoDB.
        """
        self.client.close()
        print("Connexion à MongoDB fermée.")


def main():
    flight_date = "2024-10-10"  # Date de vol à récupérer
    offset = 0

    if not API_URL or not API_KEY or not MONGO_URI or not MONGO_DBNAME:
        print("Les paramètres de l'API ou MongoDB ne sont pas correctement définis.")
        return

    # Initialiser le client API et MongoDB
    api_client = AviationAPIClient(API_URL, API_KEY)
    mongo_pipeline = MongoDBPipeline(MONGO_URI, MONGO_DBNAME)

    while True:
        # Récupérer les données de l'API
        records = api_client.fetch_data(flight_date, offset=offset)
        if not records:
            break

        # Ajouter les enregistrements au pipeline MongoDB
        mongo_pipeline.insert_data_to_mongodb(records)

        # Avancer dans la pagination
        offset += len(records)

    # Fermer la connexion MongoDB
    mongo_pipeline.close_connection()


if __name__ == "__main__":
    main()
