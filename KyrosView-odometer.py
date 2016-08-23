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
from haversine import haversine

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

JSON_DIR = config['directory_jsons']
LOG_FILE = config['directory_logs'] + "/kyrosView-odometer.log"
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
				devicesOdometer[deviceId] = {}
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getOdometerData(deviceId):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	query = """SELECT N_DAY, N_WEEK, N_MONTH, N_TOTAL, LAST_TRACKING_ID, 
		DAY_SPEED_AVERAGE, DAY_DISTANCE, DAY_HOURS, DAY_CONSUME,
		WEEK_SPEED_AVERAGE, WEEK_DISTANCE, WEEK_HOURS, WEEK_CONSUME,
		MONTH_SPEED_AVERAGE, MONTH_DISTANCE, MONTH_HOURS, MONTH_CONSUME,
		SPEED_AVERAGE, DISTANCE, HOURS, CONSUME,
		LAST_LATITUDE, LAST_LONGITUDE
		FROM ODOMETER 
		WHERE DEVICE_ID=xxx"""
	queryOdometer = query.replace('xxx', str(deviceId))
	cursor.execute(queryOdometer)
	result = cursor.fetchall()
	odometerData = {'nday': 0, 'nweek': 0, 'nmonth': 0, 'ntotal': 0, 'lastTrackingId': 0, 
		'daySpeedAverage': 0, 'dayDistance': 0,  'dayHours': 0, 'dayConsume': 0.0,
		'weekSpeedAverage': 0.0, 'weekDistance': 0,  'weekHours': 0, 'weekConsume': 0.0,
		'monthSpeedAverage': 0.0, 'monthDistance': 0,  'monthHours': 0, 'monthConsume': 0.0,
		'speedAverage': 0.0, 'distance': 0,  'hours': 0, 'consume': 0.0,
		'lastLatitude': 0.0, 'lastLongitude': 0.0}
	if (result != ()):
		odometerData = {'nday': result[0], 'nweek': result[1], 'nmonth': result[2], 'ntotal': result[3], 'lastTrackingId': result[4], 
		'daySpeedAverage': result[5], 'dayDistance': result[6],  'dayHours': result[7], 'dayConsume': result[8],
		'weekSpeedAverage': result[9], 'weekDistance': result[10],  'weekHours': result[11], 'weekConsume': result[12],
		'monthSpeedAverage': result[13], 'monthDistance': result[14],  'monthHours': result[15], 'monthConsume': result[16],
		'speedAverage': result[17], 'distance': result[18],  'hours': result[19], 'consume': result[20],
		'lastLatitude': result[21], 'lastLongitude': result[22]}
	
	return odometerData
		
def getTracking5(deviceId):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	query = """SELECT VEHICLE.DEVICE_ID as DEVICE_ID, 
		TRACKING_5.TRACKING_ID as TRACKING_ID, 
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
		WHERE VEHICLE.VEHICLE_LICENSE = TRACKING_5.VEHICLE_LICENSE and VEHICLE.DEVICE_ID=xxx order by TRACKING_5.POS_DATE desc"""
	queryTracking = query.replace('xxx', str(deviceId))
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
''' 
def saveOdometer (deviceId, newOdometerData):
INSERT INTO table (id,a,b,c,d,e,f,g)
VALUES (1,2,3,4,5,6,7,8) 
ON DUPLICATE KEY
    UPDATE a=a, b=b, c=c, d=d, e=e, f=f, g=g;
'''

print getActualTime() + " Cargando datos..."

getDevices()

print getActualTime() + " Preparando ficheros..."
os.system("rm -f " + JSON_DIR + "/devices/realTime/*.json")
openJsonFiles()

print getActualTime() + " Procesando el tracking..."

for device in devices.keys():
	trackingInfo = getTracking5(device)

	# leer datos actuales de odometro
	odometerData = getOdometerData(device)
	newOdometerData = odometerData
	indexTracking = 1
	lat2, lat3, lat4, lat5 = 0, 0, 0, 0
	lon2, lon3, lon4, lon5 = 0, 0, 0, 0
	lastPosition = (odometerData['lastLatitude'], odometerData['lastLongitude'])
	for tracking in trackingInfo:
		deviceId = tracking[0]
		trackingId = int(tracking[1])
		alias = str(tracking[2])
		latitude = tracking[3]
		longitude = tracking[4]
		speed = tracking[5]
		heading = tracking[6]
		tracking_state = str(tracking[7])
		state = str(tracking[8])
		license = str(tracking[9])
		posDate = tracking[10]

		if (indexTracking==2):
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

		#print odometerData.keys()
		oldTrackingId = 0
		try:
			oldTrackingId = int (odometerData['lastTrackingId'])
		except:
			pass
		if (trackingId> oldTrackingId):
			newOdometerData['nday'] = newOdometerData['nday'] + 1
			newOdometerData['nweek'] = newOdometerData['nweek'] + 1
			newOdometerData['nmonth'] = newOdometerData['nmonth'] + 1
			newOdometerData['ntotal'] = newOdometerData['ntotal'] + 1
			#print newOdometerData['daySpeedAverage']
			newOdometerData['daySpeedAverage'] = (newOdometerData['daySpeedAverage'] * (newOdometerData['nday']-1/newOdometerData['nday'])) + (speed * (1/newOdometerData['nday']))
			newOdometerData['weekSpeedAverage'] = (newOdometerData['weekSpeedAverage'] * (newOdometerData['nweek']-1/newOdometerData['nweek'])) + (speed * (1/newOdometerData['nweek']))
			newOdometerData['monthSpeedAverage'] = (newOdometerData['monthSpeedAverage'] * (newOdometerData['nmonth']-1/newOdometerData['nmonth'])) + (speed * (1/newOdometerData['nmonth']))

			newPosition = (latitude, longitude)
			distance = haversine(lastPosition, newPosition)
			newOdometerData['distance'] = newOdometerData['distance'] + distance
			lastPosition = newPosition
			newOdometerData['lastTrackingId'] = trackingId

		indexTracking += 1

		odometerData = {"geometry": {"type": "Point", "coordinates": [ longitude , latitude ]}, "type": "Feature", "properties":{"lat2":lat2, "lon2":lon2, "lat3":lat3, "lon3":lon3, "lat4":lat4, "lon4":lon4, "lat5":lat5, "lon5":lon5,"icon": devices[deviceId], "alias":alias, "speed": speed, "heading": heading, "tracking_state":tracking_state, "vehicle_state":state, "pos_date":posDate, "license":license, 
		"daySpeedAverage": newOdometerData['daySpeedAverage'], "weekSpeedAverage": newOdometerData['weekSpeedAverage'], "monthSpeedAverage": newOdometerData['monthSpeedAverage']}}	
		devicesOdometer[deviceId] = odometerData
		#saveOdometer (deviceId, newOdometerData)

print getActualTime() + " Generando fichero..."

for device in devices.keys():
	json.dump(devicesOdometer[device], devicesJsonFile[device], encoding='latin1')

closeJsonFiles()

print getActualTime() + " Done!"


