import csv
from elasticsearch import Elasticsearch, helpers

# Configuration Elasticsearch
ELASTICSEARCH_HOST = "http://localhost:9200"  # Remplacez par votre URI Elasticsearch
INDEX_NAME = "flights_data"  # Nom de l'index Elasticsearch

# Chemin vers le CSV transformé
CSV_FILE_PATH = "C:\\Users\\eliot\\Sup_de_Vinci\\M1\\projet_entrepot_de_donnees\\data_lake_project\\env\\output_csv\\transformation_des_donnees\\donnees transformees\\flights_data_transformed.csv"

# Connexion à Elasticsearch
es = Elasticsearch([ELASTICSEARCH_HOST])

def csv_to_elasticsearch(file_path, index_name):
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            actions = []
            for row in reader:
                action = {
                    "_index": index_name,
                    "_source": row
                }
                actions.append(action)

            # Utilisation de la méthode helpers.bulk pour importer les données
            helpers.bulk(es, actions)
            print(f"Données du fichier {file_path} importées avec succès dans l'index {index_name}.")

    except Exception as e:
        print(f"Erreur lors de l'importation : {e}")

if __name__ == "__main__":
    csv_to_elasticsearch(CSV_FILE_PATH, INDEX_NAME)
