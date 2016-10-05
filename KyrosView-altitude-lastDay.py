#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-08-24
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

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

JSON_DIR = config['directory_jsons']
LOG_FILE = config['directory_logs'] + "/kyrosView-altitude.log"
LOG_FOR_ROTATE = 10

PID = "/var/run/json-generator-kyrosview-altitude-lastday"

DB_IP = config['BBDD_host']
DB_PORT = config['BBDD_port']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

devices = {}
devicesAltitude = {}
altitudeJsonFile = {}

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('kyrosView-backend')
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
        print "Checking if json generator is already running..."
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
	global devices, altitudeJsonFile
	for k in devices.keys():
		altitudeJsonFile[k] = open(JSON_DIR + '/devices/altitude/lastDay/' + str(k) + '.json', "a+")

def closeJsonFiles():
	global altitudeJsonFile
	for k, v in altitudeJsonFile.iteritems():
		v.close()

def getDevices():
	global devices, devicesAltitude
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryDevices = """SELECT DEVICE_ID, VEHICLE_LICENSE from VEHICLE"""
			logger.debug("QUERY:" + queryDevices)
			cursor = dbConnection.cursor()
			cursor.execute(queryDevices)
			row = cursor.fetchone()
			while row is not None:
				deviceId = str(row[0])
				vehicleLicense = str(row[1])
				devices[deviceId] = vehicleLicense
				devicesAltitude[deviceId] = []
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getTracking(deviceId):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryTracking = """SELECT POS_DATE, ALTITUDE  
		FROM TRACKING 
		WHERE DEVICE_ID = xxx and POS_DATE>ddd order by POS_DATE"""
	query = queryTracking.replace('xxx', str(deviceId))
	msegLast = str((int(time.time())*1000)- 86400000)
	query = query.replace('ddd', msegLast)	
	logger.debug("QUERY:" + query)
	cursor.execute(query)
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
os.system("rm -f " + JSON_DIR + "/devices/altitude/lastDay/*.json")
openJsonFiles()

print getActualTime() + " Procesando el tracking..."

for deviceId in devices.keys():
	trackingInfo = getTracking(deviceId)
	for tracking in trackingInfo:
		posDate = tracking[0]
		altitude = tracking[1]

		data = [ posDate , altitude ]

		devicesAltitude[deviceId].append(data)

'''
trackingInfo = getTracking(6)
for tracking in trackingInfo:
	posDate = tracking[0]
	altitude = tracking[1]

	data = [ posDate , altitude ]

	devicesAltitude['6'].append(data)
'''
print getActualTime() + " Generando fichero..."

for k in devicesAltitude.keys():
	json.dump(devicesAltitude[k], altitudeJsonFile[k], encoding='latin1')

closeJsonFiles()

print getActualTime() + " Done!"


