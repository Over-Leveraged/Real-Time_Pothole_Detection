import mysql.connector
import requests


cnx = mysql.connector.connect(
    host='localhost',
    user='walsh',
    password='',
    database='PotholeDatabase'
)
cursor = cnx.cursor()


query = "SELECT id, latitude, longitude FROM pot_holes2 WHERE street IS NULL AND priority IS NULL"
cursor.execute(query)
potholes = cursor.fetchall()


for pothole in potholes:
    pothole_id = pothole[0]
    latitude = pothole[1]
    longitude = pothole[2]

   
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={APIKEY}"
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

    update_query = "UPDATE pot_holes2 SET street = %s WHERE id = %s"
    update_values = (street_name, pothole_id)
    cursor.execute(update_query, update_values)
    cnx.commit()

cursor.close()
cnx.close()
