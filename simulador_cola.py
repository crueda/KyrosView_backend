#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-19-04
# version: 1.1

##################################################################################
# version 1.0 release notes: extract data from MySQL and generate json
# Initial version
# Requisites: library python-mysqldb. To install: "apt-get install python-mysqldb"
##################################################################################

import logging, logging.handlers
import os
import json
import sys
import datetime
import calendar
import time
import MySQLdb
import json
from pymongo import MongoClient
from haversine import haversine

import pika


#### VARIABLES #########################################################

LOG_FILE = "./simulador.log"
LOG_FOR_ROTATE = 10

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('simulador_cola')
	loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG_FILE , 'midnight', 1, backupCount=10)
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	loggerHandler.setFormatter(formatter)
	logger.addHandler(loggerHandler)
	#logger.setLevel(logging.DEBUG)
	logger.setLevel(logging.INFO)
except:
	print '------------------------------------------------------------------'
	print '[ERROR] Error writing log at %s' % LOG_FILE
	print '[ERROR] Please verify path folder exits and write permissions'
	print '------------------------------------------------------------------'
	exit()
########################################################################

########################################################################


	
def getActualTime():
	now_time = datetime.datetime.now()
	format = "%H:%M:%S.%f"
	return now_time.strftime(format)

def make_unicode(input):
    if type(input) != unicode:
        input =  input.decode('utf-8', 'ignore')
        return input
    else:
        return input



########################################################################
# Funcion principal
#
########################################################################

def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.28.69'))
	channel = connection.channel()
	#channel.queue_declare(queue='TEST')
	lastLat, lastLon, distance = 0,0,0
	while True:
		with open('./ruta.csv') as fp:
			for line in fp:
				vline = line.split(',')
				#trackingId = vline[0]
				#vehicleLicense = vline[1]
				vehicleLicense = "Test_1"
				deviceId = int(vline[2])
				latitude = float(vline[3])
				longitude = float(vline[4])
				speed = float(vline[5])
				heading = float(vline[6])
				altitude = float(vline[7])
				distance = float(vline[8])
				battery = int(vline[9])
				location = vline[10]
				#posDate = vline[11]
				if (lastLat == 0):
					lastLat = latitude
					lastLon = longitude
				else:
					pos1 = (lastLat, lastLon)
					pos2 = (latitude, longitude)
					distance = haversine(pos1, pos2)
				posDate = int(time.mktime(time.gmtime()))*1000
				trackingId = posDate

				mongoTrackingData = {"pos_date" : posDate, "battery" : battery, "altitude" : altitude, "heading" : heading, "distance" : distance, 
				"location" : {"type" : "Point", "coordinates" : [longitude, latitude]},"tracking_id" : trackingId, "speed": speed, "vehicle_license" : vehicleLicense, 
				"geocoding" : location, "events" : [], "device_id" : deviceId}

				#save2Mongo (vehicleLicense, mongoTrackingData)

				msg_queue = {"msg_type": 0, "msg": mongoTrackingData}

				channel.basic_publish(exchange='',routing_key='TEST', body=json.dumps(msg_queue))

				time.sleep(2)
			

if __name__ == '__main__':
    main()

