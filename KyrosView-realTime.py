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

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

JSON_DIR = config['directory_jsons']
LOG_FILE = config['directory_logs'] + "/kyrosView-realTime.log"
LOG_FOR_ROTATE = 10

PID = "/var/run/json-generator-kyrosview"

DB_IP = config['BBDD_host']
DB_PORT = config['BBDD_port']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

monitors = {}
users = {}
iconsRealTime = {}
iconsCover = {}
iconsAlarm = {}
userJsonFile = {}
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

def openJsonFiles():
	global users, userJsonFile
	for k in users.keys():
		userJsonFile[k] = open(JSON_DIR + '/users/realTime/' + str(k) + '.json', "a+")

def closeJsonFiles():
	global userJsonFile
	for k, v in userJsonFile.iteritems():
		v.close()

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
			#query = """SELECT USERNAME, KIND_MONITOR from USER_GUI where DATE_END>ttt"""
			#queryUsers = query.replace('ttt', str(t))
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
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getTracking():
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
		round(TRACKING.GPS_SPEED,1) as speed,
		round(TRACKING.HEADING,1) as heading,
		VEHICLE.ALARM_ACTIVATED as ALARM_STATE,
		TRACKING.VEHICLE_LICENSE as DEV,
		TRACKING.POS_DATE as DATE 
		FROM VEHICLE inner join (TRACKING) 
		WHERE VEHICLE.VEHICLE_LICENSE = TRACKING.VEHICLE_LICENSE and POS_DATE>1445172885000"""
	cursor.execute(queryTracking)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbConnection.close

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
		TRACKING_5.VEHICLE_LICENSE as DEV,
		TRACKING_5.POS_DATE as DATE 
		FROM VEHICLE inner join (TRACKING_5) 
		WHERE VEHICLE.VEHICLE_LICENSE = TRACKING_5.VEHICLE_LICENSE order by TRACKING_5.DEVICE_ID, TRACKING_5.POS_DATE desc"""
	logger.debug("QUERY:" + queryTracking)
	cursor.execute(queryTracking)
	result = cursor.fetchall()
	
	try:
		return result
	except Exception, error:
		logger.error('Error getting data from database: %s.', error )
		
	cursor.close
	dbConnection.close

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
	result = cursor.fetchone()
	odometerData = {'nday': 0, 'nweek': 0, 'nmonth': 0, 'ntotal': 0, 'lastTrackingId': 0, 
		'daySpeedAverage': 0, 'dayDistance': 0,  'dayHours': 0, 'dayConsume': 0.0,
		'weekSpeedAverage': 0.0, 'weekDistance': 0,  'weekHours': 0, 'weekConsume': 0.0,
		'monthSpeedAverage': 0.0, 'monthDistance': 0,  'monthHours': 0, 'monthConsume': 0.0,
		'speedAverage': 0.0, 'distance': 0,  'hours': 0, 'consume': 0.0,
		'lastLatitude': 0.0, 'lastLongitude': 0.0}
	if (result != None):
		odometerData = {'nday': result[0], 'nweek': result[1], 'nmonth': result[2], 'ntotal': result[3], 'lastTrackingId': result[4], 
		'daySpeedAverage': result[5], 'dayDistance': result[6],  'dayHours': result[7], 'dayConsume': result[8],
		'weekSpeedAverage': result[9], 'weekDistance': result[10],  'weekHours': result[11], 'weekConsume': result[12],
		'monthSpeedAverage': result[13], 'monthDistance': result[14],  'monthHours': result[15], 'monthConsume': result[16],
		'speedAverage': result[17], 'distance': result[18],  'hours': result[19], 'consume': result[20],
		'lastLatitude': result[21], 'lastLongitude': result[22]}
	
	return odometerData

def getActualTime():
	now_time = datetime.datetime.now()
	format = "%H:%M:%S.%f"
	return now_time.strftime(format)

print getActualTime() + " Cargando datos..."

getUsers()
getIcons()
getMonitor()

print getActualTime() + " Preparando ficheros..."
os.system("rm -f " + JSON_DIR + "/users/realTime/*.json")
openJsonFiles()

print getActualTime() + " Procesando el tracking..."
trackingInfo = getTracking1()
userTracking = {}
for k in users.keys():
	userTracking [k] = []


for tracking in trackingInfo:
	deviceId = tracking[0]
	alias = str(tracking[1])
	latitude = tracking[2]
	longitude = tracking[3]
	speed = tracking[4]
	heading = tracking[5]
	state = str(tracking[6])
	license = str(tracking[7])
	posDate = tracking[8]

	odometerData = getOdometerData(deviceId)

	deviceData = {"geometry": {"type": "Point", "coordinates": [ longitude , latitude ]}, "type": "Feature", "properties":{"iconReal": iconsRealTime[deviceId], "iconCover": iconsCover[deviceId], "iconAlarm": iconsAlarm[deviceId], 
	"alias":alias, "speed": speed, "heading": heading, "vehicle_state":state, "pos_date":posDate, "license":license, "deviceId":deviceId,
	"daySpeed": odometerData['daySpeedAverage'], "weekSpeed": odometerData['weekSpeedAverage'], "monthSpeed": odometerData['monthSpeedAverage'], 
	"dayDistance": odometerData['dayDistance'], "weekDistance": odometerData['weekDistance'], "monthDistance": odometerData['monthDistance'], 
	"dayConsume": odometerData['dayConsume'], "weekConsume": odometerData['weekConsume'], "monthConsume": odometerData['monthConsume'] 
	}}	

	for username in monitors[deviceId]:
		userTracking[username].append(deviceData)



print getActualTime() + " Generando fichero..."

for k in users.keys():
	json.dump(userTracking [k], userJsonFile[k], encoding='latin1')

closeJsonFiles()

print getActualTime() + " Done!"


