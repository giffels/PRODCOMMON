#!/usr/bin/env python

import base64
import cPickle
import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Core.Codes import exceptions
from ProdCommon.Database.Connect import connect as dbConnect

# session is a tripplet identified by an id: (connection,cursor,state)
session={}
current_session='default'
current_db={}

def connect(sessionID=None):
   global session
   global current_session
   if sessionID==None:
       sessionID=current_session
   # check if cursor exists
   if not session.has_key(sessionID):
       session[sessionID]={}
       session[sessionID]['connection']=dbConnect(**current_db)
       session[sessionID]['state']='connect'
       session[sessionID]['queries']=[]

def start_transaction(sessionID=None):
   global session
   global current_db
   global current_session

   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   if not session[sessionID]['state']=='start_transaction':
       session[sessionID]['cursor']=session[sessionID]['connection'].cursor()
       if current_db['dbType']=='mysql':
           startTransaction="START TRANSACTION"
           session[sessionID]['cursor'].execute(startTransaction)
       session[sessionID]['state']='start_transaction'
   current_session=sessionID

def get_cursor(sessionID=None):
   global session
   global current_db

   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   return session[sessionID]['cursor']

   
def commit(sessionID=None):
   global session
   global current_session

   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   if session[sessionID]['state']=='connect':
       raise ProdException(exceptions[4005],4005)
   if session[sessionID]['state']!='commit':
       session[sessionID]['cursor'].execute("COMMIT")
       session[sessionID]['cursor'].close()
       session[sessionID]['state']='commit'
       session[sessionID]['queries']=[]
   

def rollback(sessionID=None):
   global session
   global current_session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   if not session[sessionID]['state']=='start_transaction':
       raise ProdException(exceptions[4003],4003)
   session[sessionID]['cursor'].execute("ROLLBACK")
   session[sessionID]['cursor'].close()
   session[sessionID]['state']='commit'
   session[sessionID]['queries']=[]

def close(sessionID=None):
   global session
   global current_session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   session[sessionID]['connection'].close()
   del session[sessionID]

def set_session(sessionID="default"):
   global session
   global current_session

   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   current_session=sessionID

def set_database(connection_parameters={}):
   global current_db
   current_db=connection_parameters
   if not current_db.has_key('dbType'):
       current_db['dbType']='mysql'

def callproc(procName,parameters={},sessionID='default'):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.debug("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   print('test '+str(dir(session[sessionID]['cursor'])))
   session[sessionID]['cursor'].callproc(procName,parameters)
  

def execute(sqlQuery,sessionID=None):
   global current_session
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.debug("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   cursor=get_cursor(sessionID)
   try:
       cursor.execute(sqlQuery)
       rowsModified=cursor.rowcount
       session[sessionID]['queries'].append(sqlQuery)
       return rowsModified
   except Exception,ex:
       # the exception might not be a los of connection
       if ex[0]==1062:
           raise ex
       logging.warning("connection to database lost "+str(ex))
       invalidate(sessionID)
       connect(sessionID)
       start_transaction(sessionID)
       logging.warning("connection recovered")
       redo()
       rowsModified=cursor.execute(sqlQuery)
       session[sessionID]['queries'].append(sqlQuery)
       return rowsModified

def fetchall(sessionID=None):       
   global current_session
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   cursor=get_cursor(sessionID)
   return cursor.fetchall()

def fetchone(sessionID=None):
   global current_session
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   cursor=get_cursor(sessionID)
   return cursor.fetchone()
       
def commit_all():
   global current_session
   global session
   for sessionID in session.keys():
       commit(sessionID)

def close_all():
   global session
   for sessionID in session.keys():
       close(sessionID)

def rollback_all():
   global session
   for sessionID in session.keys():
       rollback(sessionID)

def convert(description=[],rows=[],oneItem=False,decode=[]):
   result=[]
   for row in rows:
      row_result={}
      for i in xrange(0,len(description)):
          if description[i] in decode:
              row_result[description[i]]=cPickle.loads(base64.decodestring(row[i]))
          else:
              row_result[description[i]]=row[i]
      result.append(row_result)
   if oneItem:
      if len(result)>0:
          return result[0]
      return None
   return result

###########################################################
###  used only in this file most of the time.        #####
###########################################################

def redo(sessionID=None):
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.debug("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   cursor=get_cursor(sessionID)
   for query in session[sessionID]['queries']:
       cursor.execute(query)

def invalidate(sessionID=None):
   if sessionID==None:
       sessionID=current_session
   try:
       del session[sessionID]
   except:
       pass 

def get_cursor(sessionID=None):
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   if not session[sessionID]['state']=='start_transaction':
       start_transaction(sessionID)
   return session[sessionID]['cursor']
