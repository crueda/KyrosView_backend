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
import socket 
from pymongo import MongoClient
from haversine import haversine

import pika


#### VARIABLES #########################################################

LOG_FILE = "./simulador.log"
LOG_FOR_ROTATE = 10

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('simulador_cola')
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

KCS_HOST = "192.168.28.251"
#KCS_HOST = "127.0.0.1"
KCS_PORT = 5000

	
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


def send2kcs(imei, posDate, lon, lat, speed, heading, altitude):
	global latitudeDict, longitudeDict, speedDict, altitudeDict, distanceDict, hrDict, posDateDict
	connectionRetry = 0.5
	trama_kcs = str(imei) + ',' + str(posDate) + ',' + str(lon) + ',' + str(lat) + ',' + str(speed) + ',' + str(heading) + ',' + str(altitude) + ',9,2,0.0,0.9,3836'

	try:
		socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		socketKCS.connect((KCS_HOST, int(KCS_PORT)))
		connectedKCS = True
		socketKCS.send(trama_kcs + '\r\n')
		logger.debug ("Sent to KCS: %s " % trama_kcs)
		sendMessage = True
		socketKCS.close()
	except socket.error,v:
		logger.error('Error sending data: %s', v[0])
		try:
			socketKCS.close()
			logger.info('Trying close connection...')
		except Exception, error:
			logger.info('Error closing connection: %s', error)
			pass
			while sendMessage==False:
				try:
					logger.info('Trying reconnection to KCS...')
					socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					socketKCS.connect((KCS_HOST, int(KCS_PORT)))
					connectedKCS = True
					socketKCS.send(trama_kcs + '\r\n')
					logger.debug ("Sent to KCS: %s " % trama_kcs)
					sendMessage = True
					socketKCS.close()
				except Exception, error:
					logger.info('Reconnection to KCS failed....waiting %d seconds to retry.' , connectionRetry)
					sendMessage=False
					try:
						socketKCS.close()
					except:
						pass
					time.sleep(connectionRetry)

########################################################################
# Funcion principal
#
########################################################################

def main():
	lastLat, lastLon, distance = 0,0,0
	while True:
		with open('./ruta3.csv') as fp:
			for line in fp:
				vline = line.split(',')
				#trackingId = vline[0]
				#vehicleLicense = vline[1]
				#deviceId = int(vline[2])
				
				vehicleLicense = "Test_1"
				deviceId = 663
				imei = 109997775549

				latitude = float(vline[3])
				longitude = float(vline[4])
				speed = float(vline[5])
				heading = float(vline[6])
				altitude = float(vline[7])
				distance = float(vline[8])
				battery = int(vline[9])
				location = vline[10]
				#posDate = vline[11]
				if (lastLat == 0):
					lastLat = latitude
					lastLon = longitude
				else:
					pos1 = (lastLat, lastLon)
					pos2 = (latitude, longitude)
					distance = haversine(pos1, pos2)
				#posDate = int(time.mktime(time.gmtime()))*1000
				posDate = int(int(time.time()))*1000

				trackingId = posDate

				s = posDate / 1000.0
				nomoDate = datetime.datetime.fromtimestamp(s).strftime('%Y%m%d%H%M%S')


				send2kcs(imei, nomoDate, longitude, latitude, speed, heading, altitude)

				time.sleep(2)
			

if __name__ == '__main__':
    main()

