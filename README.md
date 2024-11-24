# Projet : Récupération et visualisation de deux APIS
 
## Technologies Utilisées
 
### Langage
 
![VScode](https://img.shields.io/badge/VScode-1.95-blue?logo=VScode&logoColor=white)
 
![Python](https://img.shields.io/badge/Python-3.13.00-blue?logo=python&logoColor=white)
 
![Docker](https://img.shields.io/badge/Docker-27.3.1-blue?logo=docker&logoColor=white)
 
![MongoDB](https://img.shields.io/badge/MongoDB-7.0.5-green?logo=mongodb&logoColor=white)
 
![Kibana](https://img.shields.io/badge/Kibana-8.15.3-orange?logo=kibana&logoColor=white)
 
---
 
## Objectif du Projet
 
L'ensemble de ces outils ont été utilisé pour le projet d'implémentation et utilisation de deux APIS. Il visent à ingérer, transformer, et analyser des données en temps réel pour obtenir des insights sur les vols d'avions (atteris, en vol ou en décolage) selon la météo en temps réel. Les données sont ensuite visualisées à l'aide d'Elasticsearch et Kibana.
 
##  Mes cibles
 
Mes cibles principales incluent :
 
- **Données francaise :** Visualisation de l'ensemble des vols en partances d'aéroport francais
 
- **Condition météorologique :** Prendre les conditions météos (Température,Pression atmosphérique, direction du vent)
 
## Architecture du projet
 
 
 
 
![alt text](ArchitecturePipeline.png)
 
### Workflow et Schéma d'Architecture
 
 
1. **Ingestion des Données Vol en temps réel (API "AVIONSTACK")** :
   - Extraction des informations sur les vols ou futures vols via l'API AVIONSTACK, envoi des données dans MongoDB.
 
1. **Ingestion des Données de météo en temps réel (API "AVIONSTACK")** :
   - Extraction des informations sur les vols ou futures vols via l'API AVIONSTACK, envoi des données dans MongoDB.
 
 
 
 
## Déroulement Technique du Projet
 
### **Étapes d'installation :**
 
 
1. **Créer un environnement virtuel :**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Unix
   # Ou
   venv\Scripts\activate     # Windows
 
2. **Installer les dépendances :**
   ```bash
   pip install -r requirements.txt
   ```
**Configurer les variables d'environnement :**
   Créez un fichier `.env` et renseignez les informations de connexion MongoDB, Airflow ainsi que les liens des deux différents APIs  :
   ```env

 MONGO_URI="*********"
MONGO_DBNAME="*********"
API_URL=http://api.aviationstack.com/v1/flights?access_key="*********"
API_KEY="*********"

MONGODB_URL="*********"
DATABASE_NAME="*********"
COLLECTION_NAME="*********"
API_URL_METEO_2=https://www.infoclimat.fr/opendata/?version=2&method=get&format=json&stations[]=07130&start=2024-01-01&end=2024-11-21&token="*********"
METEO_API_KEY="*********"


   ```
 
## Visualisation des données ou de statistique avec kibana/power BI
 
![alt text](Statistique.png)
 
Nous pouvons voir quelques statistiques ci-dessus.
Pour cette année, la Température minimale était de -6°C et maximale de 34.8°.
 
Nous avons également vu qu'il y avait une corrélation positive entre l'annulation des vols, le taux
d'humidité et la visibilité.
