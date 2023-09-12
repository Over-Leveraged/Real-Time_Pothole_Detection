import os
import mysql.connector
import requests
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import haversine_distances

folder_path = '/home/walsh/Documents/UWI/COMP3190/Scripts/Detections'

for filename in os.listdir(folder_path):
    if filename.endswith('.txt'):  
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'r') as file:
            lines = file.readlines()
            if len(lines) >= 5:
                name = lines[0].strip().split(': ')[1]
                date_taken = lines[1].strip().split(': ')[1]
                pothole_class = lines[2].strip().split(': ')[1]
                latitude = lines[3].strip().split(': ')[1]
                longitude = lines[4].strip().split(': ')[1]
            else:
                continue  

            date_taken_str = lines[1].strip().split(': ')[1]
            date_taken = datetime.strptime(date_taken_str, "%Y%m%d%H%M%S")
            date = date_taken.strftime("%Y-%m-%d")
            time = date_taken.strftime("%H:%M")


            api_key = ""
            url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={api_key}"
            response = requests.get(url)
            data = response.json()

            if data['status'] == 'OK' and len(data['results']) > 0:
                address_components = data['results'][0]['address_components']
                street_name = None
                for component in address_components:
                    if 'route' in component['types']:
                        street_name = component['long_name']
                        break
                if street_name is None:
                    street_name = "Unknown"
            else:
                street_name = "Unknown"
        with open(file_path, 'w') as file:
             file.write(f"Name: {name}\n")
             file.write(f"Class: {pothole_class}\n")
             file.write(f"Latitude: {latitude}\n")
             file.write(f"Longitude: {longitude}\n")
             file.write(f"Date: {date}\n")
             file.write(f"Time: {time}\n")
             file.write(f"Location: {street_name}\n")



def dbscanClusteringTest():
    potholes = pd.read_csv('location_data.csv')
    coordinates = potholes[['Longitude', 'Latitude']].values

    earthRadius = 6371000
    distancesMatrix = haversine_distances(np.radians(coordinates))
    #print(distances)

    degrees = 0.000009 #1 meter in degrees
    radians = 0.0000001570796327 #1 meter in radians
    epsilon = radians  
    model = DBSCAN(eps=epsilon, min_samples=1, metric='precomputed')
    clabels = model.fit_predict(distancesMatrix)
    print(model.labels_)
    uniqueLabels = np.unique(clabels[clabels != -1])
    deduplicated_potholes = pd.DataFrame(columns=potholes.columns)

    for label in uniqueLabels:
        cPotholes = potholes[clabels == label]
        deduplicated_potholes = deduplicated_potholes.append(cPotholes.iloc[0])

    deduplicated_potholes = pd.DataFrame(deduplicated_potholes)

    deduplicated_potholes.to_csv('deduplicatedpotholes.csv', index=False)