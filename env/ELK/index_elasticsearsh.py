from elasticsearch import Elasticsearch

# Connexion à Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Nom de l'index
index_name = "flights_data"

# Mapping de l'index
mapping = {
    "mappings": {
        "properties": {
            "flight_date": { "type": "date" },
            "flight_status": { "type": "keyword" },
            "departure_airport": { "type": "keyword" },
            "departure_scheduled": { "type": "date" },
            "arrival_airport": { "type": "keyword" },
            "arrival_scheduled": { "type": "date" },
            "airline_name": { "type": "text" },
            "flight_number": { "type": "keyword" },
            "aircraft_registration": { "type": "keyword" }
        }
    }
}

# Créer l'index
if not es.indices.exists(index=index_name):
    response = es.indices.create(index=index_name, body=mapping)
    print(f"Index créé : {response}")
else:
    print(f"L'index {index_name} existe déjà.")
