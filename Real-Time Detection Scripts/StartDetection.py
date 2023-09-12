
import os
import argparse
import cv2
import numpy as np
import sys
import time
from threading import Thread
import importlib.util
import datetime
import time
import random
import geocoder
import signal
from tflite_runtime.interpreter import Interpreter
from tensorflow.lite.python.interpreter import Interpreter
import requests
import json


def signal_handler(sig, frame):
    print("Script stopped.")
    sys.exit(0)


class VideoStream:
    def __init__(self,resolution=(640,480),framerate=30):
        self.stream = cv2.VideoCapture(0)
        ret = self.stream.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*'MJPG'))
        ret = self.stream.set(3,resolution[0])
        ret = self.stream.set(4,resolution[1])
        (self.grabbed, self.frame) = self.stream.read()
        self.stopped = False

    def start(self):
        Thread(target=self.update,args=()).start()
        return self

    def update(self):
        while True:
            if self.stopped:
                self.stream.release()
                return
            (self.grabbed, self.frame) = self.stream.read()

    def read(self):
        return self.frame

    def stop(self):
        self.stopped = True

# Define and parse input arguments
parser = argparse.ArgumentParser()
parser.add_argument('--modeldir', help='Folder the .tflite file is located in',
                    required=True)
parser.add_argument('--graph', help='Name of the .tflite file, if different than detect.tflite',
                    default='detect.tflite')
parser.add_argument('--labels', help='Name of the labelmap file, if different than labelmap.txt',
                    default='labelmap.txt')
parser.add_argument('--threshold', help='Minimum confidence threshold for displaying detected objects',
                    default=0.35)


args = parser.parse_args()

MODEL_NAME = args.modeldir
GRAPH_NAME = args.graph
LABELMAP_NAME = args.labels
min_conf_threshold = float(args.threshold)
resW, resH = args.resolution.split('x')
imW, imH = int(resW), int(resH)
use_TPU = args.edgetpu
pkg = importlib.util.find_spec('tflite_runtime')

CWD_PATH = os.getcwd()
PATH_TO_CKPT = os.path.join(CWD_PATH,MODEL_NAME,GRAPH_NAME)
PATH_TO_LABELS = os.path.join(CWD_PATH,MODEL_NAME,LABELMAP_NAME)
with open(PATH_TO_LABELS, 'r') as f:
    labels = [line.strip() for line in f.readlines()]

if labels[0] == '???':
    del(labels[0])

interpreter = Interpreter(model_path=PATH_TO_CKPT)

interpreter.allocate_tensors()

# Get model details
input_details = interpreter.get_input_details()
output_details = interpreter.get_output_details()
height = input_details[0]['shape'][1]
width = input_details[0]['shape'][2]

floating_model = (input_details[0]['dtype'] == np.float32)

input_mean = 127.5
input_std = 127.5
outname = output_details[0]['name']

if ('StatefulPartitionedCall' in outname): 
    boxes_idx, classes_idx, scores_idx = 1, 3, 0
else: 
    boxes_idx, classes_idx, scores_idx = 0, 1, 2

frame_rate_calc = 1
freq = cv2.getTickFrequency()
videostream = VideoStream(resolution=(imW,imH),framerate=30).start()
time.sleep(1)

while True:
    t1 = cv2.getTickCount()
    frame1 = videostream.read()
    frame = frame1.copy()
    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    frame_resized = cv2.resize(frame_rgb, (width, height))
    input_data = np.expand_dims(frame_resized, axis=0)

    if floating_model:
        input_data = (np.float32(input_data) - input_mean) / input_std

    interpreter.set_tensor(input_details[0]['index'],input_data)
    interpreter.invoke()
    
    if not os.path.exists("detection"):
        os.makedirs("detection")

    # Retrieve detection results
    boxes = interpreter.get_tensor(output_details[boxes_idx]['index'])[0] 
    classes = interpreter.get_tensor(output_details[classes_idx]['index'])[0] 
    scores = interpreter.get_tensor(output_details[scores_idx]['index'])[0] 

    for i in range(len(scores)):
        if ((scores[i] > min_conf_threshold) and (scores[i] <= 1.0)):

            ymin = int(max(1, (boxes[i][0] * imH)))
            xmin = int(max(1, (boxes[i][1] * imW)))
            ymax = int(min(imH, (boxes[i][2] * imH)))
            xmax = int(min(imW, (boxes[i][3] * imW)))

            cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), (10, 255, 0), 2)

            # Geocode IP address
            #g = geocoder.ip('me')

            # Extract latitude and longitude
            #latitude = g.lat 
            #longitude = g.lng 

            # Print the coordinates
            #print("Latitude and Longitude:", latitude, longitude)
            url = f'https://www.googleapis.com/geolocation/v1/geolocate?key={api_key}'
            payload = {
                "homeMobileCountryCode": 876,  
                "homeMobileNetworkCode": 20,   
                "radioType": "gsm",
                "carrier": "Digicel",
                "considerIp": True,
                "cellTowers": [ 
                    {
                        "cellId": 42,
                        "locationAreaCode": 415,
                        "mobileCountryCode": 310,
                        "mobileNetworkCode": 410,
                        "age": 0,
                        "signalStrength": -60,
                        "timingAdvance": 15
                    }

                ],
                "wifiAccessPoints": [
                    {
                        "macAddress": "01:23:45:67:89:AB",
                        "signalStrength": 8,
                        "age": 0,
                        "channel": 8,
                        "signalToNoiseRatio": 40
                    }
                ]
            }

            try:
                response = requests.post(url, data=json.dumps(payload))
                data = response.json()
                if 'location' in data:
                    latitude = data['location']['lat']
                    longitude = data['location']['lng']
                    print(f"Latitude: {latitude}")
                    print(f"Longitude: {longitude}")
                else:
                    print("Unable to retrieve location.")
            except requests.exceptions.RequestException as e:
                print("Error occurred:", e)
            
            current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"detection/pothole_{current_time}_{i}.jpg"
            cv2.imwrite(file_name, frame)
            print("Pothole Detected. Image saved as", file_name)
            
            txt_file_name = f"detection/pothole_{current_time}_{i}.txt"
            with open(txt_file_name, "w") as f:
                f.write(f"Name: pothole_{current_time}_{i}.jpg\n")
                f.write(f"Date Taken: {current_time}\n")
                f.write(f"Class: {labels[int(classes[i])]}\n")
                f.write(f"Latitude: {latitude}\n")
                f.write(f"Longitude: {longitude}\n")
            
            #print("Text file generated for detection:", txt_file_name)

            # Draw label
            object_name = labels[int(classes[i])] 
            label = '%s: %d%%' % (object_name, int((scores[i]*100)+10)) 
            labelSize, baseLine = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.7, 2) 
            label_ymin = max(ymin, labelSize[1] + 10) 
            cv2.rectangle(frame, (xmin, label_ymin-labelSize[1]-10), (xmin+labelSize[0], label_ymin+baseLine-10), (255, 255, 255), cv2.FILLED) # Draw white box to put label text in
            cv2.putText(frame, label, (xmin, label_ymin-7), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 2) 
    cv2.putText(frame,'FPS: {0:.2f}'.format(frame_rate_calc),(30,50),cv2.FONT_HERSHEY_SIMPLEX,1,(255,255,0),2,cv2.LINE_AA)

    # Calculate framerate
    t2 = cv2.getTickCount()
    time1 = (t2-t1)/freq
    frame_rate_calc= 1/time1
                
    signal.signal(signal.SIGINT, signal_handler)
    #cv2.imshow('Object detector', frame)
    if cv2.waitKey(1) == ord('q'):
        break

cv2.destroyAllWindows()
videostream.stop()







## Info: This script is used to detect objects using a TensorFlow Lite model.
## some aspects is based off the TFLite_detection_webcam.py script by Evan Juras.
## It has been modified to meet our needs.
