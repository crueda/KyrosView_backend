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
import json
from pymongo import MongoClient


#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./KyrosView-backend.properties')

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
monitorsFleet = {}
users = {}
usersMonitor = {}
devices = {}
iconsRealTime = {}
iconsCover = {}
iconsAlarm = {}
userTracking = {}
monitorTree = None
monitorTreeJson = None
fleetNameDict = {}
fleetParentDict = {}

monitorTree = None
monitorJson = None
fleetNameDict = {}
fleetParentDict = {}
fleetChildsDict = {}
fleetDevicesIdDict = {}
fleetDevicesLicenseDict = {}
fleetDevicesAliasDict = {}

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('kyrosView-backend')
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
        print "Checking if Kyros2Mongo is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
			print "You already have an instance of Kyros2Mongo running"
			print "It is running as process %s," % old_pd
			sys.exit(1)
        else:
			print "Trying to start Kyros2Mongo..."
			os.remove(os.path.expanduser(PID))

pidfile = open(os.path.expanduser(PID), 'a')
print "Kyros2Mongo started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()
#########################################################################

def addMonitor(deviceId, username):
	#logger.debug('  -> addMonitor: %s - %s', deviceId, username) 
	global monitors
	if (deviceId in monitors):
		monitors[deviceId].append(username) 
	else:
		monitors[deviceId] = [username]

def addMonitorFleet(fleetId, username):
	#logger.debug('  -> addMonitor: %s - %s', deviceId, username) 
	global monitorsFleet
	if (fleetId

	 in monitors):
		monitorsFleet[fleetId].append(username) 
	else:
		monitorsFleet[fleetId] = [username]

def getIcons():
	global icons
	icons = {}
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
	global users, usersMonitor
	users = {}
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
				usersMonitor[username] = {"username": username, "monitor": [] }
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getDevices():
	global devices
	devices = {}
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
	logger.debug('getMonitorSystem with username: %s', username)
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
	logger.debug('getMonitorCompany with username: %s', username)
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
	logger.debug('getMonitorFleet with username: %s', username)
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
					addMonitorFleet(fleetId, username)
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
	logger.debug('getMonitorDevice with username: %s', username)
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
	global monitors, monitorsFleet
	monitors, monitorsFleet = {}, {}
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
			dbConnection.commit()
			cursor.close
			#dbConnection.close
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
		ALTITUDE as altitude,
		DISTANCE as distance,
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
		

def getPoisSystem():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryTracking = """SELECT POI.POI_ID, 
		POI.NAME, 
		POI.LATITUDE, 
		POI.LONGITUDE, 
		POI_CATEGORY.ICON
		FROM POI inner join (POI_CATEGORY) 
		WHERE POI.CATEGORY_ID = POI_CATEGORY.CATEGORY_ID and POI_CATEGORY.TYPE=0"""
	cursor.execute(queryTracking)
	result = cursor.fetchall()			
	cursor.close
	dbConnection.close

	return result

def getConsignors():
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

		cursor = dbConnection.cursor()
		queryConsignor = """SELECT CONSIGNOR_ID,
			NAME_CONSIGNOR
			FROM CONSIGNOR
			WHERE CONSIGNOR_ID>0"""
		cursor.execute(queryConsignor)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
		
		return result

	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getFleets(consignor):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

		cursor = dbConnection.cursor()
		query = """SELECT FLEET_ID,
			DESCRIPTION_FLEET,
			PARENT_ID
			FROM FLEET
			WHERE CONSIGNOR_ID=xxx ORDER BY LEVEL"""
		queryFleets = query.replace('xxx', str(consignor))
		cursor.execute(queryFleets)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
		
		return result

	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getDevicesByFleet(fleetId):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

		cursor = dbConnection.cursor()
		query = """SELECT VEHICLE.DEVICE_ID,
			VEHICLE.VEHICLE_LICENSE,
			VEHICLE.ALIAS
			FROM VEHICLE inner join (HAS) 
			WHERE VEHICLE.DEVICE_ID = HAS.DEVICE_ID
			AND HAS.FLEET_ID=xxx"""

		queryDevices = query.replace('xxx', str(fleetId))
		cursor.execute(queryDevices)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
		
		return result

	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)


########################################################################

########################################################################

class Arbol:
    def __init__(self, elemento):
        self.hijos = []
        self.elemento = elemento

def agregarElemento(arbol, elemento, elementoPadre):
    subarbol = buscarSubarbol(arbol, elementoPadre);
    subarbol.hijos.append(Arbol(elemento))

def buscarSubarbol(arbol, elemento):
    if arbol.elemento == elemento:
        return arbol
    for subarbol in arbol.hijos:
        arbolBuscado = buscarSubarbol(subarbol, elemento)
        if (arbolBuscado != None):
            return arbolBuscado
    return None 

def profundidad(arbol):
    if len(arbol.hijos) == 0: 
        return 1
    return 1 + max(map(profundidad,arbol.hijos)) 

def grado(arbol):
    return max(map(grado, arbol.hijos) + [len(arbol.hijos)])

def ejecutarProfundidadPrimero(arbol, funcion):
    funcion(arbol.elemento)
    for hijo in arbol.hijos:
        ejecutarProfundidadPrimero(hijo, funcion)

def processTreeElement(element):
	global monitorTreeJson
	if (element!=0):
		elementParent = fleetParentDict[element]
		elementName = fleetNameDict[element]
		if (fleetChildsDict.has_key(elementParent)):
			fleetChildsDict[elementParent].append (element)
		else:
			fleetChildsDict[elementParent] = [element]
		

def generateMonitorTree():
	global monitorTree, fleetDevicesIdDict, fleetDevicesLicenseDict, fleetDevicesAliasDict
	consignorInfo = getConsignors()
	for consignor in consignorInfo:
		consignorId = consignor[0]
		consignorName = consignor[1]
		#agregarElemento(consignorName, '0', 0)
		consignorFleetInfo = getFleets(consignorId)
		for consignorFleet in consignorFleetInfo:
			fleetId = consignorFleet[0]
			fleetName = consignorFleet[1]
			fleetParent = consignorFleet[2]
			agregarElemento(monitorTree, fleetId, fleetParent)
			fleetNameDict[fleetId] = fleetName
			fleetParentDict[fleetId] = fleetParent

			#leer dispositivos y rellenar diccionarios
			devicesInfo = getDevicesByFleet(fleetId)
			for device in devicesInfo:
				deviceId = device[0]
				deviceLicense = device[1]
				deviceAlias = device[2]
				#print deviceId
				if (fleetDevicesIdDict.has_key(fleetId)):
					fleetDevicesIdDict[fleetId].append(deviceId)
					fleetDevicesLicenseDict[fleetId].append(deviceLicense)
					fleetDevicesAliasDict[fleetId].append(deviceAlias)
				else:
					fleetDevicesIdDict[fleetId] = [deviceId]
					fleetDevicesLicenseDict[fleetId] = [deviceLicense]
					fleetDevicesAliasDict[fleetId] = [deviceAlias]

def generateMonitorJson0():
	global fleetChildsDict, fleetIdDict, fleetNameDict, monitorJson
	#nivel 1
	for fleetId1 in fleetChildsDict[0]:
		if (monitorsFleet.has_key(fleetId1)):
			fleetJson1 = {"type": "fleet", "id": fleetId1, "name": fleetNameDict[fleetId1], "monitor": monitorsFleet[fleetId1], "childs": []}
		else:
			fleetJson1 = {"type": "fleet", "id": fleetId1, "name": fleetNameDict[fleetId1], "monitor": [], "childs": []}
		if (fleetDevicesIdDict.has_key(fleetId1)):
			for i in range(len(fleetDevicesIdDict[fleetId1])):
				if (monitors.has_key(fleetDevicesIdDict[fleetId1][i])):
					device = {"type": "device", "id": fleetDevicesIdDict[fleetId1][i], "name": fleetDevicesLicenseDict[fleetId1][i], "monitor": monitors[fleetDevicesIdDict[fleetId1][i]]}
				else:
					device = {"type": "device", "id": fleetDevicesIdDict[fleetId1][i], "name": fleetDevicesLicenseDict[fleetId1][i], "monitor": []}
				fleetJson1['childs'].append(device)
		if (fleetChildsDict.has_key(fleetId1)):
			#nivel 2
			for fleetId2 in fleetChildsDict[fleetId1]:
				if (monitorsFleet.has_key(fleetId2)):
					fleetJson2 = {"type": "fleet", "id": fleetId2, "name": fleetNameDict[fleetId2],  "monitor": monitorsFleet[fleetId2], "childs": []}
				else:
					fleetJson2 = {"type": "fleet", "id": fleetId2, "name": fleetNameDict[fleetId2],  "monitor": [], "childs": []}
				if (fleetDevicesIdDict.has_key(fleetId2)):
					for i in range(len(fleetDevicesIdDict[fleetId2])):
						if (monitors.has_key(fleetDevicesIdDict[fleetId2][i])):
							device = {"type": "device", "id": fleetDevicesIdDict[fleetId2][i], "name": fleetDevicesLicenseDict[fleetId2][i], "monitor": monitors[fleetDevicesIdDict[fleetId2][i]]}
						else:
							device = {"type": "device", "id": fleetDevicesIdDict[fleetId2][i], "name": fleetDevicesLicenseDict[fleetId2][i], "monitor": []}
						fleetJson2['childs'].append(device)
				if (fleetChildsDict.has_key(fleetId2)):
					#nivel 3
					for fleetId3 in fleetChildsDict[fleetId2]:
						if (monitorsFleet.has_key(fleetId3)):
							fleetJson3 = {"type": "fleet", "id": fleetId3, "name": fleetNameDict[fleetId3],  "monitor": monitorsFleet[fleetId3], "childs": []}
						else:
							fleetJson3 = {"type": "fleet", "id": fleetId3, "name": fleetNameDict[fleetId3],  "monitor": [], "childs": []}
						if (fleetDevicesIdDict.has_key(fleetId3)):
							for i in range(len(fleetDevicesIdDict[fleetId3])):
								if (monitors.has_key(fleetDevicesIdDict[fleetId3][i])):
									device = {"type": "device", "id": fleetDevicesIdDict[fleetId3][i], "name": fleetDevicesLicenseDict[fleetId3][i], "monitor": monitors[fleetDevicesIdDict[fleetId3][i]]}
								else:
									device = {"type": "device", "id": fleetDevicesIdDict[fleetId3][i], "name": fleetDevicesLicenseDict[fleetId3][i], "monitor": []}

								fleetJson3['childs'].append(device)

						fleetJson2['childs'].append(fleetJson3)

				fleetJson1['childs'].append(fleetJson2)
		monitorJson.append(fleetJson1)

iconsRealTime = {}
iconsCover = {}
iconsAlarm = {}

def generateMonitorJson():
	global fleetChildsDict, fleetIdDict, fleetNameDict, monitorJson, usersMonitor
	#nivel 1	
	for fleetId1 in fleetChildsDict[0]:
		fleetJson1 = {"type": "fleet", "element_id": fleetId1, "name": fleetNameDict[fleetId1], "state": {"checked": "false"}, "ndevices": [], "childs": []}
		ndevicesFleet1 = 0
		if (fleetDevicesIdDict.has_key(fleetId1)):
			for i in range(len(fleetDevicesIdDict[fleetId1])):
				device = {"type": "device", "element_id": fleetDevicesIdDict[fleetId1][i], "state": {"checked": "false"}, "name": fleetDevicesLicenseDict[fleetId1][i]}
				#device = {"type": "device", "element_id": fleetDevicesIdDict[fleetId1][i], "iconRealTime": iconsRealTime[fleetDevicesIdDict[fleetId1][i]], "iconCover": iconsCover[fleetDevicesIdDict[fleetId1][i]], "iconAlarm": iconsAlarm[fleetDevicesIdDict[fleetId1][i]], "state": {"checked": "false"}, "name": fleetDevicesLicenseDict[fleetId1][i]}
				fleetJson1['childs'].append(device)
				ndevicesFleet1 += 1
		if (fleetChildsDict.has_key(fleetId1)):
			#nivel 2
			for fleetId2 in fleetChildsDict[fleetId1]:
				fleetJson2 = {"type": "fleet", "id": fleetId2, "state": {"checked": "false"}, "ndevices": [], "name": fleetNameDict[fleetId2], "childs": []}
				ndevicesFleet2 = 0
				if (fleetDevicesIdDict.has_key(fleetId2)):
					for i in range(len(fleetDevicesIdDict[fleetId2])):
						device = {"type": "device", "element_id": fleetDevicesIdDict[fleetId2][i], "state": {"checked": "false"}, "name": fleetDevicesLicenseDict[fleetId2][i]}
						#device = {"type": "device", "element_id": fleetDevicesIdDict[fleetId2][i], "iconRealTime": iconsRealTime[fleetDevicesIdDict[fleetId2][i]], "iconCover": iconsCover[fleetDevicesIdDict[fleetId2][i]], "iconAlarm": iconsAlarm[fleetDevicesIdDict[fleetId2][i]], "state": {"checked": "false"}, "name": fleetDevicesLicenseDict[fleetId2][i]}
						fleetJson2['childs'].append(device)
						ndevicesFleet1 += 1
						ndevicesFleet2 += 1
				if (fleetChildsDict.has_key(fleetId2)):
					#nivel 3
					for fleetId3 in fleetChildsDict[fleetId2]:
						fleetJson3 = {"type": "fleet", "element_id": fleetId3, "state": {"checked": "false"}, "ndevices": [], "name": fleetNameDict[fleetId3], "childs": []}
						ndevicesFleet3 = 0
						if (fleetDevicesIdDict.has_key(fleetId3)):
							for i in range(len(fleetDevicesIdDict[fleetId3])):
								device = {"type": "device", "element_id": fleetDevicesIdDict[fleetId3][i], "state": {"checked": "false"}, "name": fleetDevicesLicenseDict[fleetId3][i]}
								#device = {"type": "device", "element_id": fleetDevicesIdDict[fleetId3][i], "iconRealTime": iconsRealTime[fleetDevicesIdDict[fleetId3][i]], "iconCover": iconsCover[fleetDevicesIdDict[fleetId3][i]], "iconAlarm": iconsAlarm[fleetDevicesIdDict[fleetId3][i]], "state": {"checked": "false"}, "name": fleetDevicesLicenseDict[fleetId3][i]}
								fleetJson3['childs'].append(device)
								ndevicesFleet1 += 1
								ndevicesFleet2 += 1
								ndevicesFleet3 += 1
							fleetJson3['ndevices'].append(ndevicesFleet3)
						fleetJson2['childs'].append(fleetJson3)
				fleetJson2['ndevices'].append(ndevicesFleet2)
				fleetJson1['childs'].append(fleetJson2)
		fleetJson1['ndevices'].append(ndevicesFleet1)
		monitorJson.append(fleetJson1)
		for username in users.keys():
			usersMonitor[username]['monitor'].append(fleetJson1)
			#if (monitorsFleet.has_key(fleetId1)):
			#	if username in monitorsFleet[fleetId1]:
			#		usersMonitor[username]['monitor'].append(fleetJson1)



########################################################################

########################################################################

def save2Mongo(deviceData, collectionName):
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	device_collection = db[collectionName]
	device_collection.save(deviceData)

def processTracking1():
	global monitors
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	device_collection = db['tracking1']

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

		mongoTrackingData = {"_id": deviceId, "iconReal": iconsRealTime[deviceId], "iconCover": iconsCover[deviceId], "iconAlarm": iconsAlarm[deviceId], 
		"alias":alias, "speed": speed, "heading": heading, "vehicle_state":state, "pos_date":posDate, "license":license, "deviceId":deviceId,
		"latitude": latitude, "longitude": longitude, "monitor": []
		}

		for username in monitors[deviceId]:
			mongoTrackingData['monitor'].append(username)

		#save2Mongo(mongoTrackingData, 'tracking1')
		device_collection.save(mongoTrackingData)

def processTracking5():
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	tracking5_collection = db['tracking5']
	#tracking5_collection.delete_many({})
	#tracking5_collection.remove()
	device_anterior = 0
	index_tracking = 1
	trackingInfo = getTracking5()
	for tracking in trackingInfo:
		deviceId = tracking[0]
		if (deviceId!=device_anterior):
			index_tracking = 1
		else:
			index_tracking += 1
		alias = make_unicode(str(tracking[1]))
		latitude = tracking[2]
		longitude = tracking[3]
		speed = tracking[4]
		heading = tracking[5]
		state = str(tracking[6])
		license = make_unicode(str(tracking[7]))
		posDate = tracking[8]
		trackingId = tracking[9]

		mongoTrackingData = {"_id": str(deviceId)+"-"+str(index_tracking),  
		"alias":alias, "speed": speed, "heading": heading, "vehicle_state":state, "pos_date":posDate, "license":license, "deviceId":deviceId,
		"latitude": latitude, "longitude": longitude, "trackingId": trackingId
		}

		#save2Mongo(mongoTrackingData, 'tracking5')
		tracking5_collection.save(mongoTrackingData)
		device_anterior = deviceId
	

def processTracking():
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	device_collection = db['tracking']
	
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
			altitude = tracking[5]
			distance = tracking[6]
			posDate = tracking[7]
			trackingId = tracking[8]
			newLastTrackingId = trackingId

			mongoTrackingData = {"_id": trackingId,  
			"speed": speed, "heading": heading, "altitude": altitude, "distance": distance, "pos_date": posDate, "deviceId":deviceId,
			"latitude": latitude, "longitude": longitude
			}

			#save2Mongo(mongoTrackingData, 'tracking')
			device_collection.save(mongoTrackingData)
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
	

def processPois():
	global users
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	pois_collection = db['pois']

	#procesar pois de sistema
	poisInfo = getPoisSystem()
	for poi in poisInfo:
		poiId = poi[0]
		poiName = make_unicode(poi[1])
		poiLat = poi[2]
		poiLon = poi[3]
		poiIcon = poi[4]

		mongoPoiData = {"_id": poiId, "poiName": poiName, "location": {"type": "Point", "coordinates": [poiLon, poiLat]},
		"icon": poiIcon, "monitor": []
		}
		
		for username in users.keys():
			mongoPoiData['monitor'].append(username)

		pois_collection.save(mongoPoiData)

		#for username in monitors[deviceId]:
		#	mongoTrackingData['monitor'].append(username)



def saveMonitorMongo():
	global monitorJson
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	monitor_collection = db['monitorDevice']
	monitor_collection.insert(monitorJson)

def saveMonitorUserMongo():
	global monitorJson, usersMonitor
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	monitor_collection = db['MONITOR']
	for username in users.keys():
		#userMonitorData = {"username": username, "monitor": usersMonitor[username]}
		monitor_collection.save(usersMonitor[username])


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

getUsers()
getDevices()
getIcons()


getMonitor()

monitorTree = Arbol(0)
fleetParentDict[0] = 0
fleetNameDict[0] = "root"
generateMonitorTree()

ejecutarProfundidadPrimero(monitorTree, processTreeElement)
monitorJson = []
generateMonitorJson()
#saveMonitorMongo()
#print usersMonitor['crueda']
saveMonitorUserMongo()


#processOdometer()

#print json.dumps(monitorTreeJson)

#print getActualTime()
#print (sys.argv[1])

#processPois()
#processTracking5()
#print getActualTime()

'''
logger.info (getActualTime() + " <--- datos")
getUsers()
getDevices()
getIcons()
getMonitor()
logger.info (getActualTime() + " <--- tracking1")
processTracking1()
logger.info (getActualTime() + " <--- tracking5")
#processTracking5()
logger.info (getActualTime() + " <--- tracking")
processTracking()
logger.info (getActualTime() + " <--- odometer")
processOdometer()
logger.info (getActualTime() + " <--- fin")

'''

'''
clock = 1
while True:
	if (clock==61):
		clock=1

	#logger.info ("-->" + str(clock))
	if (clock==1):
		logger.info (getActualTime() + " Cargando datos...")
		getUsers()
		getDevices()
		getIcons()
		getMonitor()
		logger.info (getActualTime() + " Cargando datos... Done!")
	if (clock%5 == 0):
		logger.info (getActualTime() + " Procesando tracking1...")
		processTracking1()
		logger.info (getActualTime() + " Procesando tracking1... Done!")
	if (clock%10 == 0):
		logger.info (getActualTime() + " Procesando tracking5...")
		processTracking5()
		logger.info (getActualTime() + " Procesando tracking5... Done!")
	if (clock%31 == 0):
		logger.info (getActualTime() + " Procesando tracking...")
		processTracking()
		logger.info (getActualTime() + " Procesando tracking... Done!")
	if (clock%20 == 0):
		logger.info (getActualTime() + " Procesando odometer...")
		processOdometer()
		logger.info (getActualTime() + " Procesando odometer...Done!")

	time.sleep(1)
	clock += 1
'''
