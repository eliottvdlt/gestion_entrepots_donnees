import pymongo
import pandas as pd
from datetime import datetime

# Configuration de MongoDB
MONGO_URI = "mongodb+srv://vaudeleteliott:lWZkRFUmZetfEdy3@clusterprojet.9d0b1.mongodb.net/"  # Remplacez par votre URI MongoDB
DATABASE_NAME = "clusterprojet"  # Nom de votre base de données
COLLECTION_NAME = "belib"  # Nom de la collection

# Dossier de sortie des CSV
OUTPUT_FOLDER = "output_csv"

def export_mongo_to_csv(limit=10):
    try:
        # Connexion à MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Récupérer un nombre limité de données de MongoDB
        documents = list(collection.find().limit(limit))
        print(f"{len(documents)} documents trouvés dans MongoDB (limite : {limit}).")

        if not documents:
            print("Aucune donnée trouvée dans la collection.")
            return

        # Convertir les documents en DataFrame Pandas
        df = pd.DataFrame(documents)

        # Supprimer l'ID MongoDB si nécessaire
        if "_id" in df.columns:
            df.drop(columns=["_id"], inplace=True)

        # Générer un nom de fichier basé sur l'heure actuelle
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{OUTPUT_FOLDER}/mongo_export_{timestamp}.csv"

        # Sauvegarder les données en CSV
        df.to_csv(file_name, index=False)
        print(f"Données exportées avec succès dans : {file_name}")

    except Exception as e:
        print(f"Erreur lors de l'exportation : {e}")

# Exécuter l'exportation avec une limite
if __name__ == "__main__":
    export_mongo_to_csv(limit=10)  # Limite de 10 documents
