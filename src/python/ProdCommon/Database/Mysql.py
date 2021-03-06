#!/usr/bin/env python


from ProdCommon.Core.Codes import exceptions
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database.Config import defaultConfig

try:
   import MySQLdb
except:
   # NOTE: we might need some mappings from error code number to 
   # NOTE: error types?
   raise RuntimeError(2,"MySQLdb could not be found. Make sure it is "+ \
                            "installed or that the path is set correctly "+ \
                            "more information at: " \
                            "http://sourceforge.net/projects/mysql-python ")
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
           if (portNr!=""):
               conn=MySQLdb.Connect(host=dbHost,db=dbName,\
                                   user=dbUser,passwd=dbPasswd, \
                                   port=int(portNr))
           else:
               conn=MySQLdb.Connect(unix_socket=socketLocation,\
                                   host=dbHost,db=dbName,\
                                   user=dbUser,passwd=dbPasswd)
           return conn
       except Exception, v:
           logging.debug("Error connecting to the database: "+str(v))
           # wait and try again.
           time.sleep(__dbWaitingTime)
   raise ProdException(exceptions[4007],4007)
       
