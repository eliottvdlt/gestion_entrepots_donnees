import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Variables d'environnement pour l'API
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

def fetch_flight_data(api_url, api_key, flight_date, offset=0):
    """
    Récupère les données de l'API AviationStack pour une date et un offset spécifiques.
    """
    try:
        params = {
            "access_key": api_key,
            "flight_date": flight_date,
            "offset": offset,
            "dep_iata": "RNS"
            

        }
        print(f"Requête envoyée avec offset={offset} pour la date {flight_date}")
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        records = data.get("data", [])
        print(f"{len(records)} enregistrements récupérés pour la date {flight_date}, offset={offset}.")
        return records
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {flight_date} : {e}")
        return []

def fetch_all_data_for_2024(api_url, api_key):
    """
    Récupère les données pour toutes les dates de l'année 2024.
    """
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    current_date = start_date

    all_records = []
    while current_date <= end_date:
        flight_date = current_date.strftime("%Y-%m-%d")
        offset = 0

        while True:
            records = fetch_flight_data(api_url, api_key, flight_date, offset)
            if not records:
                break

            all_records.extend(records)
            offset += len(records)

        current_date += timedelta(days=1)

    return all_records

def create_dataframe(records):
    """
    Transforme les données brutes en un DataFrame Pandas.
    """
    if not records:
        print("Aucune donnée à transformer en DataFrame.")
        return pd.DataFrame()
    
    return pd.DataFrame(records)

def main():
    # Récupérer toutes les données pour 2024
    records = fetch_all_data_for_2024(API_URL, API_KEY)

    # Créer un DataFrame avec les données récupérées
    df = create_dataframe(records)

    if df.empty:
        print("Aucune donnée récupérée pour l'année 2024.")
        return

    # Afficher un aperçu du DataFrame
    print("Aperçu du DataFrame :")
    print(df.head())

    # Exporter en CSV
    df.to_csv("flights_2024.csv", index=False)
    print("Les données ont été exportées dans le fichier flights_2024.csv.")

if __name__ == "__main__":
    main()
