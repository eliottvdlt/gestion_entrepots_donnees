import requests
import pandas as pd
from datetime import datetime, timedelta

# Configuration
API_URL = "http://api.aviationstack.com/v1/flights?access_key=18559336e5d7c45ac77b1fa9bb3c2254"
API_KEY = "18559336e5d7c45ac77b1fa9bb3c2254"
TARGET_AIRPORT = "Saint-Jacques"
OUTPUT_FILE = "saint_jacques_flights.csv"




def fetch_data(api_url, api_key, flight_date, offset=0):
    """
    Récupère les données de l'API AviationStack.
    """
    try:
        params = {
            'access_key': api_key,
            'flight_date': flight_date,  # Date de vol au format YYYY-MM-DD
            'offset': offset  # Pagination
        }
        print(f"Récupération des données pour la date : {flight_date}, offset={offset}")
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            json_data = response.json()
            return json_data.get('data', [])
        else:
            print(f"Erreur API ({response.status_code}): {response.text}")
            return []
    except Exception as e:
        print(f"Erreur lors de la récupération des données de l'API : {e}")
        return []

def filter_flights(records, target_airport):
    """
    Filtre les vols liés à l'aéroport cible.
    """
    filtered_records = []
    for record in records:
        try:
            departure = record.get("departure", {})
            arrival = record.get("arrival", {})

            # Vérifier si l'aéroport de départ ou d'arrivée correspond à la cible
            if (departure.get("airport") and target_airport in departure["airport"]) or \
               (arrival.get("airport") and target_airport in arrival["airport"]):
                filtered_records.append(record)
        except Exception as e:
            print(f"Erreur lors du filtrage de l'enregistrement : {record}. Erreur : {e}")
    return filtered_records

def main():
    """
    Point d'entrée principal pour récupérer, filtrer et exporter les données en CSV.
    """
    all_filtered_data = []
    current_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    while current_date <= end_date:
        flight_date = current_date.strftime('%Y-%m-%d')
        offset = 0

        while True:
            records = fetch_data(API_URL, API_KEY, flight_date, offset=offset)

            if not records:
                break

            filtered_records = filter_flights(records, TARGET_AIRPORT)  # Filtre des données
            all_filtered_data.extend(filtered_records)
            offset += len(records)

        current_date += timedelta(days=1)  # Passer au jour suivant

    if all_filtered_data:
        print(f"{len(all_filtered_data)} enregistrements filtrés trouvés pour l'aéroport : {TARGET_AIRPORT}")
        df = pd.DataFrame(all_filtered_data)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"Données filtrées exportées avec succès dans : {OUTPUT_FILE}")
    else:
        print("Aucune donnée filtrée à exporter.")

if __name__ == "__main__":
    main()
