from pymongo import MongoClient
from bson import ObjectId
from elasticsearch import Elasticsearch, helpers
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(dotenv_path="C:/Users/eliot/Sup_de_Vinci/M1/projet_entrepot_de_donnees/data_lake_project/env/airflow/dags/.env")

# Configurations MongoDB et Elasticsearch
MONGO_URI = os.getenv("MONGODB_URL")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "meteo_data"

# Connexion à MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Connexion à Elasticsearch
es = Elasticsearch(ELASTICSEARCH_URL)

def sanitize_document(doc):
    """
    Nettoie un document MongoDB pour le rendre compatible avec Elasticsearch.
    """
    sanitized = {}
    for key, value in doc.items():
        # Convertir les ObjectId en chaînes
        if isinstance(value, ObjectId):
            sanitized[key] = str(value)
        # Convertir les valeurs numériques en float/int si possible
        elif isinstance(value, str) and value.replace('.', '', 1).isdigit():
            sanitized[key] = float(value) if '.' in value else int(value)
        # Remplacer None par une chaîne vide ou une valeur par défaut
        elif value is None:
            sanitized[key] = None
        # Conserver les autres valeurs telles quelles
        else:
            sanitized[key] = value
    return sanitized

def transfer_data_to_elasticsearch():
    """
    Transfère les données de MongoDB vers Elasticsearch.
    """
    # Récupérer les données de MongoDB
    data = list(collection.find())
    print(f"{len(data)} documents trouvés dans MongoDB.")

    # Nettoyer et formater les documents pour Elasticsearch
    actions = [
        {
            "_index": INDEX_NAME,
            "_id": str(doc["_id"]),  # Utiliser l'_id MongoDB comme ID Elasticsearch
            "_source": sanitize_document(doc),  # Nettoyer les documents
        }
        for doc in data
    ]

    # Insérer les documents dans Elasticsearch
    try:
        helpers.bulk(es, actions)
        print(f"{len(actions)} documents transférés dans Elasticsearch.")
    except helpers.BulkIndexError as e:
        print("Erreur d'insertion dans Elasticsearch :")
        for error in e.errors:
            print(error)

if __name__ == "__main__":
    transfer_data_to_elasticsearch()
