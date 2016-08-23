#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-08-23
# version: 1.0

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

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

JSON_DIR = config['directory_jsons']
LOG_FILE = config['directory_logs'] + "/users/kyrosView-odometer.log"
LOG_FOR_ROTATE = 10

PID = "/var/run/json-generator-odometer-kyrosview"

DB_IP = config['BBDD_host']
DB_PORT = config['BBDD_port']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

devices = {}
devicesOdometer = {}
devicesJsonFile = {}

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('kyrosView-odometer')
	loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG_FILE , 'midnight', 1, backupCount=10)
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	loggerHandler.setFormatter(formatter)
	logger.addHandler(loggerHandler)
	logger.setLevel(logging.DEBUG)
except:
	print '------------------------------------------------------------------'
	print '[ERROR] Error writing log at %s' % LOG_FILE
	print '[ERROR] Please verify path folder exits and write permissions'
	print '------------------------------------------------------------------'
	exit()
########################################################################

########################################################################
if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if json generator odometer is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
			print "You already have an instance of the json generator running"
			print "It is running as process %s," % old_pd
			sys.exit(1)
        else:
			print "Trying to start json generator..."
			os.remove(os.path.expanduser(PID))

pidfile = open(os.path.expanduser(PID), 'a')
print "json generator started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()
#########################################################################

def openJsonFiles():
	global devices, devicesJsonFile
	for k in devices.keys():
		devicesJsonFile[k] = open(JSON_DIR + '/devices/realTime/' + str(k) + '.json', "a+")

def closeJsonFiles():
	global devicesJsonFile
	for k, v in devicesJsonFile.iteritems():
		v.close()

def getDevices():
	global devices
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryIcons = """SELECT v.DEVICE_ID, d.ICON_REAL_TIME from VEHICLE v, DEVICE d where v.ICON_DEVICE=d.ID"""
			logger.debug("QUERY:" + queryIcons)
			cursor = dbConnection.cursor()
			cursor.execute(queryIcons)
			row = cursor.fetchone()
			while row is not None:
				deviceId = row[0]
				icon = row[1]
				devices[deviceId] = icon
				devicesOdometer[deviceId] = []
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getTracking1():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryTracking = """SELECT VEHICLE.DEVICE_ID as DEVICE_ID, 
		VEHICLE.ALIAS as DRIVER, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(TRACKING_1.GPS_SPEED,1) as speed,
		round(TRACKING_1.HEADING,1) as heading,
		VEHICLE.START_STATE as TRACKING_STATE, 
		VEHICLE.ALARM_ACTIVATED as ALARM_STATE,
		TRACKING_1.VEHICLE_LICENSE as DEV,
		TRACKING_1.POS_DATE as DATE 
		FROM VEHICLE inner join (TRACKING_1) 
		WHERE VEHICLE.VEHICLE_LICENSE = TRACKING_1.VEHICLE_LICENSE"""
	cursor.execute(queryTracking)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbConnection.close

def getTracking5(deviceId):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	query = """SELECT VEHICLE.DEVICE_ID as DEVICE_ID, 
		VEHICLE.ALIAS as DRIVER, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(TRACKING_5.GPS_SPEED,1) as speed,
		round(TRACKING_5.HEADING,1) as heading,
		VEHICLE.START_STATE as TRACKING_STATE, 
		VEHICLE.ALARM_ACTIVATED as ALARM_STATE,
		TRACKING_5.VEHICLE_LICENSE as DEV,
		TRACKING_5.POS_DATE as DATE 
		FROM VEHICLE inner join (TRACKING_5) 
		WHERE VEHICLE.VEHICLE_LICENSE = TRACKING_5.VEHICLE_LICENSE and VEHICLE.DEVICE_ID=xxx order by TRACKING_5.DEVICE_ID, TRACKING_5.POS_DATE desc"""
	queryTracking = query.replace('xxx', deviceId)
	logger.debug("QUERY:" + queryTracking)
	cursor.execute(queryTracking)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbConnection.close

def getActualTime():
	now_time = datetime.datetime.now()
	format = "%H:%M:%S.%f"
	return now_time.strftime(format)

print getActualTime() + " Cargando datos..."

getDevices()

print getActualTime() + " Preparando ficheros..."
os.system("rm -f " + JSON_DIR + "/devices/realTime/*.json")
openJsonFiles()

print getActualTime() + " Procesando el tracking..."

for device in devices.keys():
	trackingInfo = getTracking5(device)

	for tracking in trackingInfo:
		deviceId = tracking[0]
		alias = str(tracking[1])
		latitude = tracking[2]
		longitude = tracking[3]
		speed = tracking[4]
		heading = tracking[5]
		tracking_state = str(tracking[6])
		state = str(tracking[7])
		license = str(tracking[8])
		posDate = tracking[9]

		odometer = {"geometry": {"type": "Point", "coordinates": [ longitude , latitude ]}, "type": "Feature", "properties":{"icon": icons[deviceId], "alias":alias, "speed": speed, "heading": heading, "tracking_state":tracking_state, "vehicle_state":state, "pos_date":posDate, "license":license}}	

		devicesOdometer[deviceId] = odometer


'''
deviceId = 0
firstElement = True
deviceIdAnterior, aliasAnterior, speedAnterior, headingAnterior, trackingStateAnterior, stateAnterior, licenseAnterior, posDateAnterior = 1, 0, 0, 0, 0, 0, 0, 0
indexTracking = 1
lat1, lat2, lat3, lat4, lat5 = 0, 0, 0, 0, 0
lon1, lon2, lon3, lon4, lon5 = 0, 0, 0, 0, 0
for tracking in trackingInfo:
	deviceId = tracking[0]
	alias = str(tracking[1])
	latitude = tracking[2]
	longitude = tracking[3]
	speed = tracking[4]
	heading = tracking[5]
	trackingState = str(tracking[6])
	state = str(tracking[7])
	license = str(tracking[8])
	posDate = tracking[9]

	if (int(deviceId) == int(deviceIdAnterior)):		
		if (indexTracking==1):
			lat1 = latitude
			lon1 = longitude
		elif (indexTracking==2):
			lat2 = latitude
			lon2 = longitude
		elif (indexTracking==3):
			lat3 = latitude
			lon3 = longitude
		elif (indexTracking==4):
			lat4 = latitude
			lon4 = longitude
		elif (indexTracking==5):
			lat5 = latitude
			lon5 = longitude
	else:
		if (firstElement==True):
			position = {"geometry": {"type": "Point", "coordinates": [ lon1 , lat1 ]}, "type": "Feature", "properties":{"lat2":lat2, "lon2":lon2, "lat3":lat3, "lon3":lon3, "lat4":lat4, "lon4":lon4, "lat5":lat5, "lon5":lon5, "icon": icons[deviceIdAnterior], "alias":aliasAnterior, "speed": speedAnterior, "heading": headingAnterior, "tracking_state":trackingStateAnterior, "vehicle_state":stateAnterior, "pos_date":posDateAnterior, "license":licenseAnterior}}	

			for username in monitors[deviceId]:
				userTracking[username].append(position)

			lat1 = latitude
			lon1 = longitude
			lat2, lat3, lat4, lat5 = 0, 0, 0, 0
			lon2, lon3, lon4, lon5 = 0, 0, 0, 0	
			indexTracking = 1	
			firstElement = False
		else:
			position = {"geometry": {"type": "Point", "coordinates": [ lon1 , lat1 ]}, "type": "Feature", "properties":{"lat2":lat2, "lon2":lon2, "lat3":lat3, "lon3":lon3, "lat4":lat4, "lon4":lon4, "lat5":lat5, "lon5":lon5, "icon": icons[deviceIdAnterior], "alias":aliasAnterior, "speed": speedAnterior, "heading": headingAnterior, "tracking_state":trackingStateAnterior, "vehicle_state":stateAnterior, "pos_date":posDateAnterior, "license":licenseAnterior}}	

			for username in monitors[deviceId]:
				userTracking[username].append(position)

			lat1 = latitude
			lon1 = longitude
			lat2, lat3, lat4, lat5 = 0, 0, 0, 0
			lon2, lon3, lon4, lon5 = 0, 0, 0, 0	
			indexTracking = 1	

	deviceIdAnterior = deviceId
	aliasAnterior = alias
	speedAnterior = speed
	headingAnterior = heading
	trackingStateAnterior = trackingState
	stateAnterior = state
	licenseAnterior = license
	posDateAnterior = posDate
	indexTracking += 1
'''

print getActualTime() + " Generando fichero..."

for device in devices.keys():
	json.dump(devicesOdometer[device], devicesJsonFile[device], encoding='latin1')

closeJsonFiles()

print getActualTime() + " Done!"


