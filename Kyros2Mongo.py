#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-07-31
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
from pymongo import MongoClient

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

JSON_DIR = config['directory_jsons']
LOG_FILE = config['directory_logs'] + "/kyros2mongo.log"
LOG_FOR_ROTATE = 10

PID = "/var/run/json-generator-kyrosview"

DB_IP = config['BBDD_host']
DB_PORT = config['BBDD_port']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

DB_MONGO_IP = config['BBDD_MONGO_host']
DB_MONGO_PORT = config['BBDD_MONGO_port']
DB_MONGO_NAME = config['BBDD_MONGO_name']

monitors = {}
users = {}
devices = {}
iconsRealTime = {}
iconsCover = {}
iconsAlarm = {}
userTracking = {}

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

def addMonitor(deviceId, username):
	logger.debug('  -> addMonitor: %s - %s', deviceId, username) 
	global monitors
	if (deviceId in monitors):
		monitors[deviceId].append(username) 
	else:
		monitors[deviceId] = [username]

def getIcons():
	global icons
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryIcons = """SELECT v.DEVICE_ID, d.ICON_REAL_TIME , d.ICON_COVER, d.ICON_ALARM from VEHICLE v, DEVICE d where v.ICON_DEVICE=d.ID"""
			logger.debug("QUERY:" + queryIcons)
			cursor = dbConnection.cursor()
			cursor.execute(queryIcons)
			row = cursor.fetchone()
			while row is not None:
				deviceId = row[0]
				iconRealTime = row[1]
				iconCover = row[2]
				iconAlarm = row[3]
				iconsRealTime[deviceId] = iconRealTime
				iconsCover[deviceId] = iconCover
				iconsAlarm[deviceId] = iconAlarm
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getUsers():
	global users
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryUsers = """SELECT USERNAME, DATE_END from USER_GUI"""
			logger.debug("QUERY:" + queryUsers)
			cursor = dbConnection.cursor()
			cursor.execute(queryUsers)
			row = cursor.fetchone()
			while row is not None:
				username = str(row[0])
				dateEnd = row[1]
				users[username] = dateEnd
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getDevices():
	global devices
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
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorSystem(username):
	logger.info('getMonitorSystem with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryDevices = """SELECT v.DEVICE_ID, d.ICON_REAL_TIME from VEHICLE v, DEVICE d where v.ICON_DEVICE=d.ID"""
			logger.debug("QUERY:" + queryDevices)
			cursor = dbConnection.cursor()
			cursor.execute(queryDevices)
			row = cursor.fetchone()
			while row is not None:
				deviceId = int(row[0])
				deviceIcon = str(row[1])
				addMonitor(deviceId, username)
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorCompany(username):
	logger.info('getMonitorCompany with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """SELECT ID from MONITORS where USERNAME='xxx'"""
			queryMonitor = query.replace('xxx', str(username))
			logger.debug("QUERY:" + queryMonitor)
			cursor = dbConnection.cursor()
			cursor.execute(queryMonitor)
			row = cursor.fetchone()
			while row is not None:
				consignorId = str(row[0])
				queryDevices = "SELECT h.DEVICE_ID from HAS h, FLEET f where h.FLEET_ID=f.FLEET_ID and f.CONSIGNOR_ID=" + consignorId
				logger.debug("QUERY:" + queryDevices)
				cursor2 = dbConnection.cursor()
				cursor2.execute(queryDevices)
				row2 = cursor2.fetchone()
				while row2 is not None:
					deviceId = row2[0]
					deviceIcon = ''
					logger.debug('  -> addMonitor: %s - %s', deviceId, deviceIcon) 
					addMonitor(deviceId, username)
					row2 = cursor2.fetchone()

				row = cursor.fetchone()
				cursor2.close
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorFleet(username):
	logger.info('getMonitorFleet with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """SELECT ID from MONITORS where USERNAME='xxx'"""
			queryMonitor = query.replace('xxx', str(username))
			logger.debug("QUERY:" + queryMonitor)
			cursor = dbConnection.cursor()
			cursor.execute(queryMonitor)
			row = cursor.fetchone()
			while row is not None:
				fleetId = str(row[0])
				queryDevices = "SELECT DEVICE_ID from HAS where FLEET_ID=" + fleetId
				cursor2 = dbConnection.cursor()
				cursor2.execute(queryDevices)
				row2 = cursor2.fetchone()
				while row2 is not None:
					deviceId = row2[0]
					deviceIcon = ''
					addMonitor(deviceId, username)
					row2 = cursor2.fetchone()

				row = cursor.fetchone()
				cursor2.close
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorDevice(username):
	logger.info('getMonitorDevice with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
			query = """SELECT ID from MONITORS where USERNAME=='xxx'"""
			queryMonitor = query.replace('xxx', str(username))
			cursor = dbConnection.cursor()
			cursor.execute(queryMonitor)
			row = cursor.fetchall()
			while row is not None:
				deviceId = row[0]
				addMonitor(deviceId, username)
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	
def getMonitor():
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())*1000
			queryUsers = """SELECT USERNAME, KIND_MONITOR from USER_GUI"""
			logger.debug("QUERY:" + queryUsers)
			cursor = dbConnection.cursor()
			cursor.execute(queryUsers)
			row = cursor.fetchone()
			while row is not None:
				username = row[0]
				kindMonitor = int(row[1])
				if (kindMonitor==0):
					getMonitorCompany(username)
				elif (kindMonitor==1):
					getMonitorFleet(username)
				elif (kindMonitor==2):
					getMonitorSystem(username)
				elif (kindMonitor==3):
					getMonitorDevice(username)
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getLastTrackingId(deviceId):
	lastTrackingId = 0
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """SELECT LAST_TRACKING_ID_MONGO from VEHICLE where DEVICE_ID=xxx"""
			queryLastTrackingId = query.replace('xxx' , str(deviceId))
			logger.debug("QUERY:" + queryLastTrackingId)
			cursor = dbConnection.cursor()
			cursor.execute(queryLastTrackingId)
			row = cursor.fetchone()
			if (row is not None):
				lastTrackingId = row[0]
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	return lastTrackingId

def updateLastTrackingId(deviceId, trackingId):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """UPDATE VEHICLE set LAST_TRACKING_ID_MONGO=ttt where DEVICE_ID=xxx"""
			queryLastTrackingId = query.replace('xxx' , str(deviceId)).replace('ttt', str(trackingId))
			logger.debug("QUERY:" + queryLastTrackingId)
			cursor = dbConnection.cursor()
			cursor.execute(queryLastTrackingId)
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

########################################################################

########################################################################

def getTracking1():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryTracking = """SELECT VEHICLE.DEVICE_ID as DEVICE_ID, 
		VEHICLE.ALIAS as ALIAS, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(TRACKING_1.GPS_SPEED,1) as speed,
		round(TRACKING_1.HEADING,1) as heading,
		VEHICLE.ALARM_ACTIVATED as ALARM_STATE,
		VEHICLE.VEHICLE_LICENSE as DEV,
		TRACKING_1.POS_DATE as DATE, 
		TRACKING_1.TRACKING_ID as TRACKING_ID 
		FROM VEHICLE inner join (TRACKING_1) 
		WHERE VEHICLE.DEVICE_ID = TRACKING_1.DEVICE_ID"""
	cursor.execute(queryTracking)
	result = cursor.fetchall()			
	cursor.close
	dbConnection.close

	return result

def getTracking5():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryTracking = """SELECT VEHICLE.DEVICE_ID as DEVICE_ID, 
		VEHICLE.ALIAS as ALIAS, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(TRACKING_5.GPS_SPEED,1) as speed,
		round(TRACKING_5.HEADING,1) as heading,
		VEHICLE.ALARM_ACTIVATED as ALARM_STATE,
		VEHICLE.VEHICLE_LICENSE as DEV,
		TRACKING_5.POS_DATE as DATE,
		TRACKING_5.TRACKING_ID as TRACKING_ID 
		FROM VEHICLE inner join (TRACKING_5) 
		WHERE VEHICLE.DEVICE_ID = TRACKING_5.DEVICE_ID order by TRACKING_5.DEVICE_ID, TRACKING_5.POS_DATE desc"""
	logger.debug("QUERY:" + queryTracking)
	cursor.execute(queryTracking)
	result = cursor.fetchall()		
	cursor.close
	dbConnection.close

	return result

def getTracking(deviceId, trackingId):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	query = """SELECT TRACKING.DEVICE_ID as DEVICE_ID, 
		round(POS_LATITUDE_DEGREE,5) + round(POS_LATITUDE_MIN/60,5) as LAT, 
		round(POS_LONGITUDE_DEGREE,5) + round(POS_LONGITUDE_MIN/60,5) as LON, 
		round(TRACKING.GPS_SPEED,1) as speed,
		round(TRACKING.HEADING,1) as heading,
		TRACKING.POS_DATE as DATE,
		TRACKING.TRACKING_ID as TRACKING_ID 
		FROM TRACKING
		WHERE TRACKING.DEVICE_ID=xxx and TRACKING.TRACKING_ID>ttt order by TRACKING.POS_DATE"""
	queryTracking = query.replace('xxx', str(deviceId)).replace('ttt', str(trackingId))
	cursor.execute(queryTracking)
	result = cursor.fetchall()
	cursor.close
	dbConnection.close
	
	return result
		

def getOdometerData():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryOdometer = """SELECT DEVICE_ID, 
		DAY_SPEED_AVERAGE, WEEK_SPEED_AVERAGE, MONTH_SPEED_AVERAGE,
		round(DAY_DISTANCE,1), round(WEEK_DISTANCE,1), round(MONTH_DISTANCE,1),
		DAY_CONSUME, WEEK_CONSUME, MONTH_CONSUME
		FROM ODOMETER"""
	cursor.execute(queryOdometer)
	result = cursor.fetchall()
	cursor.close
	dbConnection.close
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		

########################################################################

########################################################################

def save2Mongo(deviceData, collectionName):
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	device_collection = db[collectionName]
	device_collection.save(deviceData)

def processTracking1():
	trackingInfo = getTracking1()
	for tracking in trackingInfo:
		deviceId = tracking[0]
		alias = make_unicode(str(tracking[1]))
		latitude = tracking[2]
		longitude = tracking[3]
		speed = tracking[4]
		heading = tracking[5]
		state = str(tracking[6])
		license = make_unicode(str(tracking[7]))
		posDate = tracking[8]
		trackingId = tracking[9]

		mongoTrackingData = {"_id": trackingId, "iconReal": iconsRealTime[deviceId], "iconCover": iconsCover[deviceId], "iconAlarm": iconsAlarm[deviceId], 
		"alias":alias, "speed": speed, "heading": heading, "vehicle_state":state, "pos_date":posDate, "license":license, "deviceId":deviceId,
		"latitude": latitude, "longitude": longitude, "monitor": []
		}

		for username in monitors[deviceId]:
			mongoTrackingData['monitor'].append(username)

		save2Mongo(mongoTrackingData, 'tracking1')

def processTracking5():
	trackingInfo = getTracking5()
	for tracking in trackingInfo:
		deviceId = tracking[0]
		alias = make_unicode(str(tracking[1]))
		latitude = tracking[2]
		longitude = tracking[3]
		speed = tracking[4]
		heading = tracking[5]
		state = str(tracking[6])
		license = make_unicode(str(tracking[7]))
		posDate = tracking[8]
		trackingId = tracking[9]

		mongoTrackingData = {"_id": trackingId,  
		"alias":alias, "speed": speed, "heading": heading, "vehicle_state":state, "pos_date":posDate, "license":license, "deviceId":deviceId,
		"latitude": latitude, "longitude": longitude
		}

		save2Mongo(mongoTrackingData, 'tracking5')

def processTracking():
	global devices
	for deviceId in devices.keys():
		lastTrackingId = getLastTrackingId(deviceId)
		trackingInfo = getTracking(deviceId, lastTrackingId)
		newLastTrackingId = lastTrackingId
		for tracking in trackingInfo:
			deviceId = tracking[0]
			latitude = tracking[1]
			longitude = tracking[2]
			speed = tracking[3]
			heading = tracking[4]
			posDate = tracking[5]
			trackingId = tracking[6]
			newLastTrackingId = trackingId

			mongoTrackingData = {"_id": trackingId,  
			"speed": speed, "heading": heading, "pos_date":posDate, "deviceId":deviceId,
			"latitude": latitude, "longitude": longitude
			}

			save2Mongo(mongoTrackingData, 'tracking5')
		updateLastTrackingId(deviceId, newLastTrackingId)
		
def processOdometer():
	odometerInfo = getOdometerData()
	for odometer in odometerInfo:

		mongoOdometerData = {"_id": odometer[0], "deviceId":odometer[0],
		"daySpeed": odometer[1], "weekSpeed": odometer[2], "monthSpeed": odometer[3], 
		"dayDistance": odometer[4], "weekDistance": odometer[5], "monthDistance": odometer[6], 
		"dayConsume": odometer[7], "weekConsume": odometer[8], "monthConsume": odometer[9]
		}

		save2Mongo(mongoOdometerData, 'odometer')
	

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

########################################################################

print getActualTime() + " Cargando datos..."

getUsers()
getDevices()
#users['crueda'] = 0
getIcons()
#getMonitor()
#print monitors[6]

print getActualTime() + " Procesando el tracking..."

#processTracking1()
#processTracking5()
#processOdometer()
processTracking()




print getActualTime() + " Done!"


