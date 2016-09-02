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
from __future__ import division

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
			query = """SELECT DEVICE_ID, CONSUMPTION from VEHICLE"""
			logger.debug("QUERY:" + query)
			cursor = dbConnection.cursor()
			cursor.execute(query)
			row = cursor.fetchone()
			while row is not None:
				deviceId = row[0]
				consumption = row[1]
				devices[deviceId] = consumption
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
		DAY_SPEED_AVERAGE, DAY_DISTANCE, DAY_CONSUME,
		WEEK_SPEED_AVERAGE, WEEK_DISTANCE, WEEK_CONSUME,
		MONTH_SPEED_AVERAGE, MONTH_DISTANCE,  MONTH_CONSUME,
		SPEED_AVERAGE, DISTANCE, CONSUME,
		LAST_LATITUDE, LAST_LONGITUDE
		FROM ODOMETER 
		WHERE DEVICE_ID=xxx"""
	queryOdometer = query.replace('xxx', str(deviceId))
	cursor.execute(queryOdometer)
	result = cursor.fetchone()
	odometerData = {'nday': 0, 'nweek': 0, 'nmonth': 0, 'ntotal': 0, 'lastTrackingId': 0, 
		'daySpeedAverage': 0, 'dayDistance': 0, 'dayConsume': 0.0,
		'weekSpeedAverage': 0.0, 'weekDistance': 0, 'weekConsume': 0.0,
		'monthSpeedAverage': 0.0, 'monthDistance': 0, 'monthConsume': 0.0,
		'speedAverage': 0.0, 'distance': 0,  'consume': 0.0,
		'lastLatitude': 0.0, 'lastLongitude': 0.0}
	if (result != None):
		odometerData = {'nday': result[0], 'nweek': result[1], 'nmonth': result[2], 'ntotal': result[3], 'lastTrackingId': result[4], 
		'daySpeedAverage': result[5], 'dayDistance': result[6], 'dayConsume': result[7],
		'weekSpeedAverage': result[8], 'weekDistance': result[9], 'weekConsume': result[10],
		'monthSpeedAverage': result[11], 'monthDistance': result[12], 'monthConsume': result[13],
		'speedAverage': result[14], 'distance': result[15], 'consume': result[16],
		'lastLatitude': result[17], 'lastLongitude': result[18]}
	
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
		round(GPS_SPEED,1) as speed,
		round(DISTANCE,1) as distance
		FROM TRACKING
		WHERE TRACKING_ID>ttt and DEVICE_ID=xxx order by TRACKING.POS_DATE"""
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
			query = """INSERT INTO ODOMETER (DEVICE_ID,SPEED_AVERAGE,DAY_SPEED_AVERAGE,WEEK_SPEED_AVERAGE,MONTH_SPEED_AVERAGE,DISTANCE,DAY_DISTANCE,WEEK_DISTANCE,MONTH_DISTANCE,CONSUME,DAY_CONSUME,WEEK_CONSUME,MONTH_CONSUME) 
			VALUES (xxx,t_sss,d_sss,w_sss,m_sss,t_ddd,d_ddd,w_ddd,m_ddd,t_ccc,d_ccc,w_ccc,m_ccc) ON DUPLICATE KEY 
			UPDATE SPEED_AVERAGE=t_sss, DAY_SPEED_AVERAGE=d_sss, WEEK_SPEED_AVERAGE=w_sss, MONTH_SPEED_AVERAGE=m_sss, 
			DISTANCE=t_ddd, DAY_DISTANCE=d_ddd, WEEK_DISTANCE=w_ddd, MONTH_DISTANCE=m_ddd,
			CONSUME=t_ccc, DAY_CONSUME=d_ccc, WEEK_CONSUME=w_ccc, MONTH_CONSUME=m_ccc"""

			queryOdometer = query.replace('xxx',str(deviceId))
			queryOdometer = queryOdometer.replace('t_sss',str(round(newOdometerData['speedAverage'],2)))				
			queryOdometer = queryOdometer.replace('d_sss',str(round(newOdometerData['daySpeedAverage'],2)))			
			queryOdometer = queryOdometer.replace('w_sss',str(round(newOdometerData['weekSpeedAverage'],2)))				
			queryOdometer = queryOdometer.replace('m_sss',str(round(newOdometerData['monthSpeedAverage'],2)))	
			queryOdometer = queryOdometer.replace('t_ddd',str(newOdometerData['distance']))			
			queryOdometer = queryOdometer.replace('d_ddd',str(newOdometerData['dayDistance']))			
			queryOdometer = queryOdometer.replace('w_ddd',str(newOdometerData['weekDistance']))			
			queryOdometer = queryOdometer.replace('m_ddd',str(newOdometerData['monthDistance']))	
			queryOdometer = queryOdometer.replace('t_ccc',str(round(newOdometerData['consume'],1)))			
			queryOdometer = queryOdometer.replace('d_ccc',str(round(newOdometerData['dayConsume'],1)))			
			queryOdometer = queryOdometer.replace('w_ccc',str(round(newOdometerData['weekConsume'],1)))			
			queryOdometer = queryOdometer.replace('m_ccc',str(round(newOdometerData['monthConsume'],1)))	
			#print queryOdometer		
			logger.debug("QUERY:" + queryOdometer)
			cursor = dbConnection.cursor()
			cursor.execute(queryOdometer)
			dbConnection.commit()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)	



print getActualTime() + " Cargando datos..."

#getDevices()
devices[6] = 10
from decimal import *
getcontext().prec = 6

print getActualTime() + " Procesando el tracking y actualizando el odometro..."

for deviceId in devices.keys():
	# leer datos actuales de odometro
	odometerData = getOdometerData(deviceId)

	trackingInfo = getTracking(deviceId, odometerData['lastTrackingId'])

	newOdometerData = odometerData
	for tracking in trackingInfo:
		trackingId = int(tracking[0])
		latitude = tracking[1]
		longitude = tracking[2]
		speed = tracking[3]
		distance = (tracking[4]/1000)

		#print odometerData.keys()
		newOdometerData['nday'] = newOdometerData['nday'] + 1
		newOdometerData['nweek'] = newOdometerData['nweek'] + 1
		newOdometerData['nmonth'] = newOdometerData['nmonth'] + 1
		newOdometerData['ntotal'] = newOdometerData['ntotal'] + 1

		old_speed = float(newOdometerData['speedAverage'])
		new_speed = float(speed)
		newOdometerData['speedAverage'] = (old_speed * ((newOdometerData['ntotal']-1)/newOdometerData['ntotal'])) + (new_speed * (1/newOdometerData['ntotal']))
		newOdometerData['daySpeedAverage'] = (newOdometerData['daySpeedAverage'] * ((newOdometerData['nday']-1)/newOdometerData['nday'])) + (speed * (1/newOdometerData['nday']))
		newOdometerData['weekSpeedAverage'] = (newOdometerData['weekSpeedAverage'] * ((newOdometerData['nweek']-1)/newOdometerData['nweek'])) + (speed * (1/newOdometerData['nweek']))
		newOdometerData['monthSpeedAverage'] = (newOdometerData['monthSpeedAverage'] * ((newOdometerData['nmonth']-1)/newOdometerData['nmonth'])) + (speed * (1/newOdometerData['nmonth']))

		newOdometerData['distance'] = newOdometerData['distance'] + distance
		newOdometerData['dayDistance'] = newOdometerData['dayDistance'] + distance
		newOdometerData['weekDistance'] = newOdometerData['weekDistance'] + distance
		newOdometerData['monthDistance'] = newOdometerData['monthDistance'] + distance

		newOdometerData['consume'] = (newOdometerData['distance']/100) * devices[deviceId]
		newOdometerData['dayConsume'] = (newOdometerData['dayDistance']/100) * devices[deviceId]
		newOdometerData['weekConsume'] = (newOdometerData['weekDistance']/100) * devices[deviceId]
		newOdometerData['monthConsume'] = (newOdometerData['monthDistance']/100) * devices[deviceId]

		newOdometerData['lastTrackingId'] = trackingId

	saveOdometer (deviceId, newOdometerData)

print getActualTime() + " Done!"


