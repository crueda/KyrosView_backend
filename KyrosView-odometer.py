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
	print queryOdometer
	cursor.execute(queryOdometer)
	result = cursor.fetchone()
	odometerData = {'nday': 0, 'nweek': 0, 'nmonth': 0, 'ntotal': 0, 'lastTrackingId': 0, 
		'daySpeedAverage': 0, 'dayDistance': 0,  'dayHours': 0, 'dayConsume': 0.0,
		'weekSpeedAverage': 0.0, 'weekDistance': 0,  'weekHours': 0, 'weekConsume': 0.0,
		'monthSpeedAverage': 0.0, 'monthDistance': 0,  'monthHours': 0, 'monthConsume': 0.0,
		'speedAverage': 0.0, 'distance': 0,  'hours': 0, 'consume': 0.0,
		'lastLatitude': 0.0, 'lastLongitude': 0.0}
	print result
	if (result != None):
		odometerData = {'nday': result[0], 'nweek': result[1], 'nmonth': result[2], 'ntotal': result[3], 'lastTrackingId': result[4], 
		'daySpeedAverage': result[5], 'dayDistance': result[6],  'dayHours': result[7], 'dayConsume': result[8],
		'weekSpeedAverage': result[9], 'weekDistance': result[10],  'weekHours': result[11], 'weekConsume': result[12],
		'monthSpeedAverage': result[13], 'monthDistance': result[14],  'monthHours': result[15], 'monthConsume': result[16],
		'speedAverage': result[17], 'distance': result[18],  'hours': result[19], 'consume': result[20],
		'lastLatitude': result[21], 'lastLongitude': result[22]}
	
	return odometerData
		
def getTracking(deviceId, lastTrackingId):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	query = """SELECT TRACKING_ID as TRACKING_ID, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(GPS_SPEED,1) as speed
		TRACKING.POS_DATE as DATE 
		FROM TRACKING
		WHERE (TRACKING_ID=ttt or TRACKING_ID>ttt) and DEVICE_ID=xxx order by TRACKING.POS_DATE"""
	queryTracking = query.replace('ttt', str(lastTrackingId)).replace('xxx', str(deviceId))
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

def saveOdometer (deviceId, newOdometerData):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """INSERT INTO ODOMETER (DEVICE_ID,DAY_SPEED_AVERAGE,WEEK_SPEED_AVERAGE,MONTH_SPEED_AVERAGE) VALUES (xxx,d_sss,w_sss,m_sss) ON DUPLICATE KEY UPDATE DAY_SPEED_AVERAGE=d_sss, WEEK_SPEED_AVERAGE=w_sss, MONTH_SPEED_AVERAGE=m_sss"""

			queryOdometer = query.replace('xxx',str(deviceId))
			queryOdometer = queryOdometer.replace('d_sss',str(newOdometerData['daySpeedAverage']))			
			queryOdometer = queryOdometer.replace('w_sss',str(newOdometerData['weekSpeedAverage']))			
			queryOdometer = queryOdometer.replace('m_sss',str(newOdometerData['monthSpeedAverage']))			
			logger.debug("QUERY:" + queryOdometer)
			cursor = dbConnection.cursor()
			cursor.execute(queryOdometer)
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)	



print getActualTime() + " Cargando datos..."

#getDevices()
devices[6] = 'a'

print getActualTime() + " Procesando el tracking y actualizando el odometro..."

for device in devices.keys():
	# leer datos actuales de odometro
	odometerData = getOdometerData(device)

	trackingInfo = getTracking(device, odometerData.lastTrackingId)

	newOdometerData = odometerData
	trackingIndex=1
	lastPosition = (0,0)
	for tracking in trackingInfo:
		trackingId = int(tracking[1])
		latitude = tracking[2]
		longitude = tracking[3]
		speed = tracking[4]
		posDate = tracking[5]

		# la primera posicion solo se usa para conocer el primer tracking
		if ( trackingIndex==1):
			lastPosition = (latitude, longitude)
		else:
			#print odometerData.keys()
			newOdometerData['nday'] = newOdometerData['nday'] + 1
			newOdometerData['nweek'] = newOdometerData['nweek'] + 1
			newOdometerData['nmonth'] = newOdometerData['nmonth'] + 1
			newOdometerData['ntotal'] = newOdometerData['ntotal'] + 1
			#print newOdometerData['daySpeedAverage']
			newOdometerData['speedAverage'] = (newOdometerData['speedAverage'] * (newOdometerData['nTotal']-1/newOdometerData['nTotal'])) + (speed * (1/newOdometerData['nTotal']))
			newOdometerData['daySpeedAverage'] = (newOdometerData['daySpeedAverage'] * (newOdometerData['nday']-1/newOdometerData['nday'])) + (speed * (1/newOdometerData['nday']))
			newOdometerData['weekSpeedAverage'] = (newOdometerData['weekSpeedAverage'] * (newOdometerData['nweek']-1/newOdometerData['nweek'])) + (speed * (1/newOdometerData['nweek']))
			newOdometerData['monthSpeedAverage'] = (newOdometerData['monthSpeedAverage'] * (newOdometerData['nmonth']-1/newOdometerData['nmonth'])) + (speed * (1/newOdometerData['nmonth']))

			newPosition = (latitude, longitude)
			distance = haversine(lastPosition, newPosition)
			newOdometerData['distance'] = newOdometerData['distance'] + distance
			newOdometerData['dayDistance'] = newOdometerData['dayDistance'] + distance
			newOdometerData['weekDistance'] = newOdometerData['weekDistance'] + distance
			newOdometerData['monthDistance'] = newOdometerData['monthDistance'] + distance
			lastPosition = newPosition

		indexTracking += 1
		newOdometerData['lastTrackingId'] = trackingId

	saveOdometer (deviceId, newOdometerData)

print getActualTime() + " Done!"


