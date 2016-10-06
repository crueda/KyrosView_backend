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


#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

LOG_FILE = config['directory_logs'] + "/sumulador.log"
LOG_FOR_ROTATE = 10

PID = "/var/run/simulador-kyrosview"

DB_IP = config['BBDD_host']
DB_PORT = config['BBDD_port']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

DB_MONGO_IP = config['BBDD_MONGO_host']
DB_MONGO_PORT = config['BBDD_MONGO_port']
DB_MONGO_NAME = config['BBDD_MONGO_name']

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('simulador')
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
if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if simulador is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
			print "You already have an instance of simulador running"
			print "It is running as process %s," % old_pd
			sys.exit(1)
        else:
			print "Trying to start simulador..."
			os.remove(os.path.expanduser(PID))

pidfile = open(os.path.expanduser(PID), 'a')
print "simulador started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()
#########################################################################


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


def getTracking():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	cursor = dbConnection.cursor()
	queryTracking = """SELECT DEVICE_ID,
		VEHICLE_LICENSE, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5), 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5), 
		round(GPS_SPEED,1),
		round(HEADING,1),
		ALTITUDE,
		DISTANCE,
		BATTERY,
		LOCATION,
		POS_DATE as DATE,
		TRACKING_ID as TRACKING_ID 
		FROM TRACKING where DEVICE_ID=655 and POS_DATE>1475311662000 and POS_DATE<1475743662000"""
	cursor.execute(queryTracking)
	result = cursor.fetchall()
	cursor.close
	dbConnection.close
	return result

########################################################################
# Funcion principal
#
########################################################################

def saveTracking():
	fichero = open('./ruta.csv', 'w')
	trackingInfo = getTracking()
	for tracking in trackingInfo:
		deviceId = tracking[0]
		vehicleLicense = tracking[1]
		latitude = tracking[2]
		longitude = tracking[3]
		speed = tracking[4]
		heading = tracking[5]
		altitude = tracking[6]
		distance = tracking[7]
		battery = tracking[8]
		location = make_unicode(tracking[9])
		posDate = tracking[10]
		trackingId = tracking[11]

		fichero.writelines('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\r\n' %(str(trackingId), str(vehicleLicense), str(deviceId), str(latitude), str(longitude), str(speed), str(heading), str(altitude), str(distance), str(battery), str(location), str(posDate)  ) )
	fichero.close

def save2Mongo(vehicleLicense, mongoTrackingData):
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]

	collectionToSave = 'TRACKING_' + str(vehicleLicense)
	collection = db[collectionToSave]
	collection.insert(mongoTrackingData)

def main():
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
				altitude = int(vline[7])
				distance = int(vline[8])
				battery = int(line[9])
				location = vline[10]
				#posDate = vline[11]
				posDate = int(time.mktime(time.gmtime()))*1000
				trackingId = posDate

				mongoTrackingData = {"pos_date" : posDate, "battery" : battery, "altitude" : altitude, "heading" : heading, 
				"location" : {"type" : "Point", "coordinates" : [longitude, latitude]},"tracking_id" : trackingId, "vehicle_license" : vehicleLicense, 
				"geocoding" : location, "events" : [], "device_id" : deviceId}

				save2Mongo (vehicleLicense, mongoTrackingData)

				time.sleep(5)
			

if __name__ == '__main__':
    #saveTracking()
    main()


#time.sleep(1)
