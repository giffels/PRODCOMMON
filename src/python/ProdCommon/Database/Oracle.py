#!/usr/bin/env python

import logging

from ProdCommon.Core.Codes import exceptions
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database.Config import defaultConfig

try:
   import cx_Oracle
except:
   logging.debug(exceptions[4008])
   logging.debug(exceptions[4008])

import time
import logging

# Try to connect a maximum of 5 times.
__maxConnectionAttempts=int(defaultConfig['maxConnectionAttempts'])
# Time to wait to reconnect
__dbWaitingTime=int(defaultConfig['dbWaitingTime'])

def connect(dbName,dbHost,dbUser,dbPasswd,socketLocation,portNr=""):

   """
   _connect_

   Generic connect method that returns a connection opbject.
   """
   for attempt in range(__maxConnectionAttempts):
       try:
           conn=cx_Oracle.connect(dbUser+'/'+dbPasswd)
           return conn
       except Exception, v:
           logging.debug("Error connecting to the database: "+str(v))
           # wait and try again.
           time.sleep(__dbWaitingTime)
   raise ProdException(exceptions[4007],4007)
       
