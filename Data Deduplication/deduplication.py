import mysql.connector
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import haversine_distances
import os
import mysql.connector
from datetime import datetime

def removeDuplication():
    cnx = mysql.connector.connect(
        host='localhost',
        user='walsh',
        password='',
        database='PotholeDatabase'
    )
    cursor = cnx.cursor()
    query_before = "SELECT COUNT(*) FROM pot_holes2"
    cursor.execute(query_before)
    count_before = cursor.fetchone()[0]

    query = "SELECT * FROM pot_holes2"
    cursor.execute(query)
    potholesData = cursor.fetchall()
    coordinates = np.array([(float(row[4]), float(row[5])) for row in potholesData])
    distances = haversine_distances(np.radians(coordinates))

    degrees = 0.000009 #1 meter in degrees
    radians = 0.0000001570796327 #1 meter in radians
    dbscan = DBSCAN(eps=radians, min_samples=1, metric='precomputed')
    clabels = dbscan.fit_predict(distances)
    unique_labels = np.unique(clabels[clabels != -1])

    deduplicated_potholes = []
    for label in unique_labels:
        cpotholes = [potholesData[i] for i, l in enumerate(clabels) if l == label]
        deduplicated_potholes.append(cpotholes[0])

    delete_query = "DELETE FROM pot_holes2;"
    cursor.execute(delete_query)
    insert_query = "INSERT INTO pot_holes2 (id, filename, date_taken, pothole_class, latitude, longitude, Street, Priority) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    cursor.executemany(insert_query, deduplicated_potholes)
    cnx.commit()
    query_after = "SELECT COUNT(*) FROM pot_holes2"
    cursor.execute(query_after)
    count_after = cursor.fetchone()[0]
    print("Records before deduplication:", count_before)
    print("Records after deduplication:", count_after)

    cursor.close()
    cnx.close()





def readDetectionFiles():
    cnx = mysql.connector.connect(
        host='localhost',
        user='walsh',
        password='',
        database='PotholeDatabase'
    )
    cursor = cnx.cursor()

    folder_path = '/home/walsh/Documents/UWI/COMP3190/Scripts/Detections'

    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):  
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as file:
                lines = file.readlines()
                if len(lines) >= 7:
                    name = lines[0].strip().split(': ')[1]
                    pothole_class = lines[1].strip().split(': ')[1]
                    latitude = lines[2].strip().split(': ')[1]
                    longitude = lines[3].strip().split(': ')[1]
                    date_str = lines[4].strip().split(': ')[1]
                    time_str = lines[5].strip().split(': ')[1]
                    location = lines[6].strip().split(': ')[1]
                else:
                    continue

                date_taken = datetime.strptime(date_str, "%Y-%m-%d")
                time_taken = datetime.strptime(time_str, "%H:%M")

                # Insert the data into the database
                insert_query = "INSERT INTO pot_holes2 (filename, date_taken, pothole_class, latitude, longitude, Street, Priority) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                values = (name, date_taken, pothole_class, latitude, longitude, location, 'Medium')

                cursor.execute(insert_query, values)
                cnx.commit()

    # Close the database connection
    cursor.close()
    cnx.close()
