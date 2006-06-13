#!/usr/bin/env python


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

# Cache (connection pool) so we can reuse connections.
__connectionCache={}
# Refresh connections every 4 hours
__refreshPeriod=int(defaultConfig['refreshPeriod'])
# Try to connect a maximum of 5 times.
__maxConnectionAttempts=int(defaultConfig['maxConnectionAttempts'])
# Time to wait to reconnect
__dbWaitingTime=int(defaultConfig['dbWaitingTime'])
# Set check connectivity period
__checkConnectionPeriod = (__maxConnectionAttempts * __dbWaitingTime) / 2

def connect(dbName,dbHost,dbUser,dbPasswd,socketLocation,portNr="",cache=True):

   """

   _connect_

   Generic connect method that returns a connection opbject.
   We do not need to close this object as we can reuse it.
   We do however have a refresh period to prevent the connection 
   from actually "cutting" us of.
   """
   cacheKey=dbName+dbHost+dbUser+dbPasswd
   if __connectionCache.has_key(cacheKey):
       conn, staleness, lastCheck = __connectionCache[cacheKey]
       timeDiff=time.time()-staleness
       if(timeDiff > __refreshPeriod) or not cache:
           conn.close()
           del __connectionCache[cacheKey] 
       else:
           # test if the connection is still open?
           # NOTE: is there a better way to do this?
           # NOTE: checking the connection using a cursor
           # NOTE: can be time consuming and 
           # NOTE: defeats the use of a cache.
           # NOTE: what I put in here is a hybrid solution.
           timeDiff=time.time()-lastCheck
           if(timeDiff > __checkConnectionPeriod):
              try:
                 cursor = conn.cursor()
                 cursor.execute("SELECT CURDATE()")
                 cursor.close()
                 return conn
              except:
                 pass
           else:
              return conn
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
           __connectionCache[cacheKey]=(conn,time.time(),time.time())
           return conn
       except Exception, v:
           # wait and try again.
           time.sleep(__dbWaitingTime)
   raise RuntimeError(1,"Could not connect to database")
       
