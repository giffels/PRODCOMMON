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
lost_connection_statements=[]

def connect(sessionID=None):
   global session
   global current_session
   if sessionID==None:
       sessionID=current_session
   # check if cursor exists
   if not session.has_key(sessionID):
       logging.debug("Establishing session")
       session[sessionID]={}
       logging.debug("Creating connection object")
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
       logging.debug("Creating cursor object for session: "+str(sessionID))
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
       execute("COMMIT",sessionID)
       session[sessionID]['cursor'].close()
       session[sessionID]['state']='commit'
       session[sessionID]['queries']=[]
   logging.debug("Transaction committed")
   

def rollback(sessionID=None):
   global session
   global current_session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       raise ProdException(exceptions[4002],4002)
   if not session[sessionID]['state']=='start_transaction':
       raise ProdException(exceptions[4003],4003)
   execute("ROLLBACK",sessionID)
   session[sessionID]['cursor'].close()
   session[sessionID]['state']='commit'
   session[sessionID]['queries']=[]
   logging.debug("Transaction rolled back")

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
       logging.warning("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   session[sessionID]['cursor'].callproc(procName,parameters)
  

def execute(sqlQuery,sessionID=None):
   global current_session
   global session
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.warning("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   cursor=get_cursor(sessionID)
   try:
       cursor.execute(sqlQuery)
       rowsModified=cursor.rowcount
       session[sessionID]['queries'].append(sqlQuery)
       return rowsModified
   except Exception,ex:
       # the exception might not be a lost of connection
       if(ex.args) and (len(ex.args)>1):
           if ex.args[0]==0:
               logging.warning("Connection to database with session (case 1) '"+str(sessionID)+"' lost. Problem: "+str(ex))
           else:
               logging.warning("Connection to database with session (case 2)'"+str(sessionID)+"' lost. Problem: "+str(ex))
       else:
           logging.warning("Connection to database with session (case 3)'"+str(sessionID)+"' lost. Problem: "+str(ex))
       invalidate(sessionID)
       connect(sessionID)
       start_transaction(sessionID)
       logging.warning("Connection recovered")
       redo()
       rowsModified=session[sessionID]['cursor'].execute(sqlQuery)
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

def convert(description=[],rows=[],oneItem=False,decode=[],decodeb64=[]):
   global current_session
   if description==[]:
      cursor=get_cursor(current_session)
      for x in cursor.description:
          description.append(x[0])

   result=[]
   for row in rows:
      row_result={}
      for i in xrange(0,len(description)):
         if description[i] in decode:
            row_result[description[i]]=cPickle.loads(base64.decodestring(row[i]))
         elif description[i] in decodeb64:
            row_result[description[i]]=base64.decodestring(row[i])
         else:
            row_result[description[i]]=row[i]
      result.append(row_result)
   if oneItem:
      if len(result)>0:
          return result[0]
      return None

   return result

def insert(table_name,rows,columns=[],encode=[],encodeb64=[],duplicate=False):
   """
   _insert_

   Generic way of inserting rows into a table. The column names
   are based on the keys of the rows dictionary or list.

   """

   # make sure everything is in the right format
   if type(rows) == dict:
      rows = [rows]
   # if nothing to do return
   if len(rows) == 0:
      return

   if type(rows[0]) == dict:
      column_names = rows[0].keys()
   if type(rows[0]) == list:
      column_names = columns

   logging.debug("Inserting objects of type %s" % table_name)

   sqlStr = "INSERT INTO %s (" % table_name
   comma = False
   for column_name in column_names:
      if comma:
         sqlStr += ', '
      comma = True
      sqlStr += column_name
   sqlStr += ')'
   sqlStr += ' VALUES '

   row_comma = False
   for row in rows:
      if row_comma:
         sqlStr += ', '
      row_comma = True
      sqlStr += '('
      entry_comma = False
      if type(rows[0]) == dict:
         for column_name in column_names:
            if column_name in encode:
               entry = base64.encodestring(cPickle.dumps(row[column_name]))
            elif column_name in encodeb64:
               entry = base64.encodestring(row[column_name])
            else:
               entry = row[column_name]
               if entry_comma:
                  sqlStr += ', '
               entry_comma = True
               if entry == "LAST_INSERT_ID()":
                  sqlStr += entry
               else:
                  sqlStr += "'%s'" % str(entry)
         sqlStr += ')'
      else:
         i=0
         for column_name in column_names:
            if column_name in encode:
               entry = base64.encodestring(cPickle.dumps(row[i]))
            elif column_name in encodeb64:
               entry = base64.encodestring(row[i])
            else:
               entry = row[i]
            if entry_comma:
               sqlStr += ', '
            entry_comma = True
            if entry == "LAST_INSERT_ID()":
               sqlStr += str(entry)
            else:
               sqlStr += "'%s'" % str(entry)
            i += 1
         sqlStr += ')'
   if duplicate:
      sqlStr += " ON DUPLICATE KEY UPDATE "
      comma = False
      for column_name in column_names:
         if comma:
            sqlStr += ', '
         elif not comma :
            comma=True
         sqlStr += "%s = '%s'" % (column_name,str(row[column_name]))

   execute(sqlStr)

def retrieve(table_name,values,columns=[],decode=[],decodeb64=[]):
   """
   __retrieve__

   Generic retrieve table rows as dictionaries, where it is
   assumed that the key of tabe is labeled "id"
   """

   # if nothing to do return
   if len(values) == 0:
      return

   logging.debug("Retrieving objects of type %s" % table_name)

   sqlStr = 'SELECT '
   if ( len(columns) == 0 ):
      sqlStr+= '*'
   else:
      comma = False
      for column_name in columns.iterkeys():
         if comma:
            sqlStr += ', '
         comma = True
         sqlStr += column_name
   sqlStr += ' FROM %s WHERE ' % table_name

   entry_and=False
   for value_name in values.keys():
      if entry_and:
         sqlStr += 'and '
      entry_and=True
      sqlStr += value_name
      if type(values[value_name]) == list:
         sqlStr += " in %s" % str(tuple(values[value_name]))
      else:
         sqlStr += " = '%s'" % values[value_name]

   execute(sqlStr)

   return convert(description=[],rows=fetchall(),oneItem=False,decode=[],decodeb64=[])



###########################################################
###  used only in this file most of the time.        #####
###########################################################

def redo(sessionID=None):
   global lost_connection_statements

   logging.warning("Trying to redo sql statements")
   if sessionID==None:
       sessionID=current_session
   if not session.has_key(sessionID):
       logging.warning("Connection not available, trying to connect")
       connect(sessionID)
       start_transaction(sessionID)
   cursor=get_cursor(sessionID)
   logging.warning("Recover "+str(len(lost_connection_statements))+" lost sql statements")
   session[sessionID]['queries']=lost_connection_statements
   for query in session[sessionID]['queries']:
       logging.debug("Re-instantiating: "+query)
       cursor.execute(query)
   logging.warning("Redo is successful")

def invalidate(sessionID=None):
   global lost_connection_statements

   if sessionID==None:
       sessionID=current_session
   logging.warning("Backing up lost sql statements")
   lost_connection_statements=session[sessionID]['queries']
   logging.warning("Invalidate session '"+sessionID+"' removing connection and cursor ")
   try:
       logging.warning("Trying to close connection and cursor (if possible)")
       try:
           session[sessionID]['cursor'].close()
           logging.warning("Successful in closing cursor")
       except Exception,ex:
           logging.warning("Unsuccessful in closing cursor: "+str(ex))
       try:
           session[sessionID]['connection'].close()
           logging.warning("Successful in closing connection")
       except Exception,ex:
           logging.warning("Unsuccessful in closing connection: "+str(ex))
           pass
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

