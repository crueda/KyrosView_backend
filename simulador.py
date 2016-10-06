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
	


########################################################################
# Funcion principal
#
########################################################################

def main():
	#while True:
		#time.sleep(DEFAULT_SLEEP_TIME)
    runkeeperKyros = getRunkeeperKyrosData()
    for data in runkeeperKyros:
    	deviceId = data[0]
    	authorization = data[1]
    	typeActivity = data[2]
    	lastActivityDate = data[3]
    	result = getImei(deviceId)
    	imei = result[0]
    	
    	processNewActivities(authorization, deviceId, imei, typeActivity, lastActivityDate)
		

if __name__ == '__main__':
    main()


#time.sleep(1)
