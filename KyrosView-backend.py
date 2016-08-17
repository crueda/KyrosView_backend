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

#http://librosweb.es/libro/algoritmos_python/capitulo_9/utilizando_diccionarios_en_python.html

import logging, logging.handlers
import os
import json
import sys
import datetime
import calendar
import time

#### VARIABLES #########################################################
from configobj import ConfigObj
config = ConfigObj('./kyrosView-backend.properties')

LOG_FILE = config['directory_logs'] + "/kyrosView-backend.log"
LOG_FOR_ROTATE = 10

PID = "/var/run/json-generator-timing"

DB_IP = config['BBDD_host']
DB_PORT = config['BBDD_port']
DB_NAME = config['BBDD_name']
DB_USER = config['BBDD_username']
DB_PASSWORD = config['BBDD_password']

monitors = {}

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
	global monitors
	if (deviceId in monitors):
		monitors[deviceId].append(username) 
	else:
		monitors[deviceId] = [username]


def getMonitorSystem(username):
	logger.debug('getMonitorSystem with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
			queryDevices = """SELECT v.DEVICE_ID, d.ICON_REAL_TIME from VEHICLE v, DEVICE d where v.ICON_DEVICE=d.ID"""
		    print queryDevices
		    cursor = dbConnection.cursor()
		    cursor.execute(queryDevices)
			row = cursor.fetchall()
		    while row is not None:
		    	deviceId = row[0]
		    	deviceIcon = row[1]
		    	logger.debug('-> %s - %s', deviceId, deviceIcon) 
		    	addMonitor(deviceId, username)

		    	row = cursor.fetchone()
		    dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorCompany(username):
	logger.debug('getMonitorCompany with username: %s', username)

def getMonitorFleet(username):
	logger.debug('getMonitorFleet with username: %s', username)

def getMonitorDevice(username):
	logger.debug('getMonitorDevice with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
			query = """SELECT ID from MONITORS where USERNAME=='xxx'"""
		    queryMonitor = query.replace('xxx', str(username))
		    print queryMonitor
		    cursor = dbConnection.cursor()
		    cursor.execute(queryMonitor)
			row = cursor.fetchall()
		    while row is not None:
		    	deviceId = row[0]


		    	row = cursor.fetchone()
		    dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitor():
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
			query = """SELECT USERNAME, KIND_MONITOR from USER_GUI where DATE_END<ttt"""
		    queryUsers = query.replace('ttt', str(t))
		    print queryUsers
		    cursor = dbConnection.cursor()
		    cursor.execute(queryUsers)
			row = cursor.fetchall()
		    while row is not None:
		    	username = row[0]
		    	kindMonitor = row[1]
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

getMonitor()
print monitors
