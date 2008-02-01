#!/usr/bin/env python
"""
_Connection_

Connection class for accessing the database backend and formatting
the input and output of database results.

The main reasons for having this class is to separate out the operations part from the connection part. 
Moreover it enables recovery when a connection brakes and secondary to abstract out the dabase specific part
and to improve connection  management

"""

import base64
import cPickle
import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Core.Codes import exceptions
from ProdCommon.Database.Connect import connect as dbConnect

#Assigns unique id to every new connection object
connection_id = 1

class Connection:

    """
    Connection object for access to multiple databases. Currently it is not threadsafe. You can still open
    multiple connections to multiple databases.
    """    
    

    def __init__(self,connection_parameters = {}):
        # connection is a tripplet identified by an id: (connection, cursor, state)
        global connection_id
          
        self.connection = {}           
         
        self.currentConnection = 'connectionId_' + str(connection_id)
        self.currentCursor = 'default'
        self.lost_connection_statements = {}
        self.currentDB = connection_parameters
          
	self.connect()
        connection_id = connection_id + 1
         
               

    def connect(self, connectionID = None, db = {}):
        """
        _connect_
 
        input: connectionID (string)
    
        output: nothing
    
        Opens (creates) a connection object if it has not been created yet.
        The connection object uses the db parameters (e.g. password, user name,
        etc...

        The dbConnect class is a wrapper for the different database backends.
        We assume the backends implement the python dbapi standard.

        The database connection parameters need to contain the type of the database
        'dbType' . Currently it supports 'mysql' and 'oracle'.

        The connection itself can contain multiple cursors. If nothing is specified when
        accessing the database a default cursur will be used. Thus if you do 
        'start_transaction' and the cursor is not there it creates it.
        
        """
    
        if connectionID == None:
            connectionID = self.currentConnection
        # check if connection exists
        if not self.connection.has_key(connectionID):
            logging.debug("Establishing connection")
            self.connection[connectionID] = {}
            logging.debug("Creating connection object")
              
            if db != {}:
                self.connection[connectionID]['connection'] = dbConnect(**db)
            else:
                
                self.connection[connectionID]['connection'] = \
                    dbConnect(**self.currentDB)
            self.connection[connectionID]['state'] = 'connected'
            self.connection[connectionID]['cursors'] = {}
 
    def start_transaction(self, connectionID = None, cursorID = None, cursorClass = None):
        """
        _start_transaction_
    
        input: connectionID (string)
    
        output: nothing
    
        Starts a database transaction. If we have connections to multiple databases open
        (through the connect method), we can specify this here.
        """
        
        global currentCursor
        
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor

        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
    
        if not self.connection[connectionID]['cursors'].has_key(cursorID) :
            
            logging.debug("Creating cursor object " + cursorID + \
                " for connection: "+str(connectionID))
            self.connection[connectionID]['cursors'][cursorID]  = {}
            if not cursorClass:
                self.connection[connectionID]['cursors'][cursorID]['cursor']  = \
                    self.connection[connectionID]['connection'].cursor()
            else:
                self.connection[connectionID]['cursors'][cursorID]['cursor']  = \
                    self.connection[connectionID]['connection'].cursor(cursorclass=cursorClass)
            self.connection[connectionID]['cursors'][cursorID]['queries']  = []
            self.connection[connectionID]['cursors'][cursorID]['state']  = 'connected'
        if not self.connection[connectionID]['cursors'][cursorID]['state'] == \
            'start_transaction':
            if self.currentDB['dbType'] == 'mysql':
                startTransaction = "START TRANSACTION"
                self.connection[connectionID]['cursors'][cursorID]['cursor'].execute(startTransaction)
            self.connection[connectionID]['cursors'][cursorID]['state'] = \
                'start_transaction'
        self.currentConnection = connectionID
        self.currentCursor = cursorID
         
 
    def commit(self, connectionID = None, cursorID = None):
        """
        _commit_

        input: connectionID (string)
    
        output: nothing

        Commits the transaction for a particular connection (connection)
        """
        
        
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor

        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
        if not self.connection[connectionID]['cursors'].has_key(cursorID):
            raise ProdException(exceptions[4013], 4013)
    
        self.execute("COMMIT", connectionID, cursorID)
        self.connection[connectionID]['cursors'][cursorID]['queries'] = []
        self.connection[connectionID]['cursors'][cursorID]['state'] = 'commit'
        logging.debug("Transaction committed for connection/cursor: " \
        + connectionID +'/'+ cursorID)
    
    def rollback(self, connectionID = None, cursorID = None):
        """
        _rollback_

        input: connectionID (string)
    
        output: nothing
    
        Rollsback a particular transaction associated to a connection 
        (connection)
        """

        global currentCursor
    
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor
    
        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
        if not self.connection[connectionID]['cursors'].has_key(cursorID):
            raise ProdException(exceptions[4013], 4013)

        self.execute("ROLLBACK", connectionID, cursorID)
        self.connection[connectionID]['cursors'][cursorID]['queries'] = []
        self.connection[connectionID]['cursors'][cursorID]['state'] = 'rollback'
        logging.debug("Transaction rolled back")
 
    def close(self, connectionID = None):
        """
        _close_
    
        input: connectionID (string)
    
        output: nothing
      
        Closes the connection
        """

        if connectionID == None:
            connectionID = self.currentConnection
        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
    
        for cursorID in self.connection[connectionID]['cursors'].keys():
            try:
                self.connection[connectionID]['cursors'][cursorID]['cursor'].close()
            except Exception,ex:
                logging.warning("Problem closing connection/cursor: " +\
                     connectionID+ "/" +cursorID)
        self.connection[connectionID]['connection'].close()
        del self.connection[connectionID]
 
    def set_connection(self, connectionID = "default"):
        """
        _set_connection_
    
        input: connectionID (string)
    
        output: nothing
    
        Sets the connection. You can for example have multiple connections 
        (connections)
        open to a (or multiple) databases. This method enables you to switch 
        between them.
        """
        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
        self.currentConnection = connectionID

    def set_cursor(self, cursorID = "default"):
        """
        _set_connection_
    
        input: connectionID (string)
    
        output: nothing
    
        Sets the connection. You can for example have multiple connections 
        (connections) open to a (or multiple) databases. This method enables 
        you to switch between them.
        """
    
        if not self.connection[self.currentConnection]['cursors'].has_key(cursorID):
            raise ProdException(exceptions[4013], 4013)
        self.currentCursor = cursorID 
 
    def set_database(self, connection_parameters ):
        """
        _set_database_
    
        input: database connection parameters
    
        output: nothing
    
        Sets the database we want to use. If we set a database we can start
        creating connections (connections). We can set different databases and
        start multiple connections through mutliple different connections.
        """
    
        self.currentDB = connection_parameters
        if not self.currentDB.has_key('dbType'):
            self.currentDB['dbType'] = 'mysql'
 
    def execute(self, sqlQuery, connectionID = None, cursorID = None):
        """
        _execute_
    
        input: sql Query and connectionID
    
        output: rows modified or number of rows retrieved.
    
        Executes the actual query in a particular connection and if query fails
        due to connection or cursor loss, will try to restore connection/cursor
        and redo all queries since the last commit.
        """
        
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor
    
        if not self.connection.has_key(connectionID):
            logging.warning("Connection " + connectionID + \
                " not available,  trying to connect")
            self.connect(connectionID)
            self.start_transaction(connectionID, cursorID)
        if not self.connection[connectionID]['cursors'].has_key(cursorID):
            logging.warning("Cursor "+ cursorID + \
                " not availalbe, trying to create")
            self.start_transaction(connectionID, cursorID)

        try:
            
            cursor = self.get_cursor(connectionID, cursorID)
            
            cursor.execute(sqlQuery)
            rowsModified = cursor.rowcount
            self.connection[connectionID]['cursors'][cursorID]['queries'].append(sqlQuery)
            
            return rowsModified
        except Exception, ex:
            # the exception might not be a lost of connection
            if(ex.args) and (len(ex.args)>1):
                if ex.args[0] == 0:
                    msg = """
Connection to database with connection (case 1) %s
 lost. Problem: %s
                    """ % (str(connectionID), str(ex))
                else:
                    msg = """
Connection to database with connection (case 1) %s
 lost. Problem: %s
                """ % (str(connectionID), str(ex))
            else:
                msg = """
Connection to database with connection (case 3) %s
 lost. Problem: %s
                """ % (str(connectionID), str(ex))
            logging.warning(msg)
            self.invalidate(connectionID)
            self.connect(connectionID)
            self.redo()
            logging.warning("Connection recovered")
            logging.warning("Cursor objects recreated")
            rowsModified = self.connection[connectionID]['cursors'][cursorID]['cursor'].execute(sqlQuery)
            self.connection[connectionID]['cursors'][cursorID]['queries'].append(sqlQuery)
            return rowsModified
    
    def fetchall(self, connectionID = None, cursorID = None, dictionary = False, \
        decode = [], decode64 = []):       
        """
        _fetchall_
    
        inpput: connectionID
    
        output: list of database rows that are the results form the last query.
        """
    
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor
    
        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
        if not self.connection[connectionID]['cursors'].has_key(cursorID):
            raise ProdException(exceptions[4013], 4013)

        cursor = self.get_cursor(connectionID, cursorID)
        if dictionary :
            return convert(cursor.fetchall(), False, decode, decode64)
        return cursor.fetchall()
 
    def fetchone(self, connectionID = None, cursorID = None, dictionary = False, \
        decode = [], decode64 = []):
        """
        _fetchone_
     
        input: connectionID, dictionary (wether output should be a dictionary),
        and decoding. If there are columns that need to be decoded.

        output: one database row that is the results form the last query.
        """
    
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor

        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
        if not self.connection[connectionID]['cursors'].has_key(cursorID):
            raise ProdException(exceptions[4013], 4013)

        cursor = self.get_cursor(connectionID, cursorID)
        if dictionary :
            return convert([cursor.fetchone()], True, decode, decode64)
        return cursor.fetchone()
        
    def commit_all(self):
        """
        _commit_all_
    
        Commits all open connections and associated cursors.
        Note: use this with caution. If any threads are relying on this 
        connection, do not use it.
        """
        for connectionID in self.connection.keys():
            individual = False
            for cursorID in self.connection[connectionID]['cursors'].keys():
                self.commit(connectionID, cursorID)
                self.connection[connectionID]['cursors'][cursorID]['queries'] = \
                    []
                self.connection[connectionID]['cursors'][cursorID]['state'] =  \
                   'commit'
 
    def close_all(self):
        """
        _close_all_
    
        Closes all open connections.
        Note: use this with caution. If any threads are relying on this 
        connection, do not use it.
        """

        for connectionID in self.connection.keys():
            self.close(connectionID)
 
    def rollback_all(self):
        """
        _rollback_all
       
        Rollsback all connections.
        """
        for connectionID in self.connection.keys():
            for cursorID in self.connection[connectionID]['cursors'].keys():
                try:
                    rollback(connectionID, cursorID)
                    self.connection[connectionID]['cursors'][cursorID]['queries'] = \
                        []
                    self.connection[connectionID]['cursors'][cursorID]['state'] = \
                        'rollback'
                except Exception,ex:
                    msg = """
Problem rolling back for connection/cursor: %s/%s
                    """ % (connectionID, cursorID)
                    logging.warning(msg) 

    def convert(self, description = None , rows = [], oneItem = False, decode = [], decode64 = []):
        """
        _convert_
    
        A generic converter that converts the result of a query (rows)
        into a list of dictionaries based on the description list.
        If oneItem is set to True it will only return a dictionary of 
        one result.
        Otherwise it returns a list (array). The decode list can contain a list
        of strings that match some of the description items, and specify that
        a particular field needs to be base64 decoded and pickled.
	Built_in function 'dict(zip(map1,map2))' functions like this:
	zip(description, rows)
	      row_result={}
              for i in xrange(0,len(description)):
                 row_result[description[i]]=row[i]
              
	   return row_result   
        """
        if description is None:
           description = []

        if description == []:
           cursor = self.get_cursor(self.currentConnection)
           description = [ d[0] for d in cursor.description ]
 
        result = []
        for row in rows:
            result.append(dict(zip(description,row)))
        if len(decode) > 0 or len(decode64) > 0:
            for i in xrange(0,len(result)):
                for attr in decode:
                    result[i][attr] =  \
                    cPickle.loads(base64.decodestring(result[i][attr]))
                for attr in decode64:
                    result[i][attr] =  \
                    base64.decodestring(result[i][attr])
        if oneItem:
            if len(result)>0:
                return result[0]
            return None
        return result


###########################################################
###  used only in this file most of the time.        #####
###########################################################

    def redo(self, connectionID = None):

        if connectionID == None:
            connectionID = self.currentConnection
        if not self.connection.has_key(connectionID):
            logging.warning("Connection not available,  trying to connect")
            self.connect(connectionID)

        total = 0 
        logging.warning("Trying to establish lost cursors")
        for cursorID in self.lost_connection_statements.keys():
            logging.warning("Establishing cursor : "+cursorID)
            self.start_transaction(connectionID, cursorID)
            logging.warning("Trying to redo sql statements")
            cursor = self.get_cursor(connectionID, cursorID)
            logging.warning("Recover " + \
                str(len(self.lost_connection_statements[cursorID])) + \
                " lost sql statements")
            total += len(self.lost_connection_statements[cursorID])
            self.connection[connectionID]['cursors'][cursorID]['queries'] = \
                self.lost_connection_statements[cursorID]
            for query in \
                self.connection[connectionID]['cursors'][cursorID]['queries']:
                logging.debug("Recovering: "+query)
                cursor.execute(query)
            logging.warning("Redo is successful for cursor: "+cursorID)

        logging.warning("****Recovered total of: "+str(total) + \
             " sql statements*****")
        self.lost_connection_statements = {}
 
    def invalidate(self, connectionID = None):

        self.lost_connection_statements = {} 
        if connectionID == None:
            connectionID = self.currentConnection

        total = 0
        for cursorID in self.connection[connectionID]['cursors'].keys():
            self.lost_connection_statements[cursorID] = \
                self.connection[connectionID]['cursors'][cursorID]['queries']
            total += len(self.connection[connectionID]['cursors'][cursorID]['queries'])

        logging.warning("****Backing up "+ str(total)+" lost sql statements****")
        logging.warning("Invalidating connection '" + \
            connectionID + "' removing connection and associated cursors ")
        try:
            logging.warning("Trying to close connection and cursor (if possible)")
            for cursorID in self.connection[connectionID]['cursors'].keys():
                logging.warning("Attempting to close cursor: "+cursorID)
                try:
                    self.connection[connectionID]['cursors'][cursorID]['cursor'].close()
                    logging.warning("Successful in closing cursor")
                except Exception, ex:
                    logging.warning("Unsuccessful in closing cursor: " + str(ex))
                del self.connection[connectionID]['cursors'][cursorID]
            logging.warning("Attempting to close connection: "+connectionID)
            try:
                self.connection[connectionID]['connection'].close()
                logging.warning("Successful in closing connection")
            except Exception, ex:
                logging.warning("Unsuccessful in closing connection: " + str(ex))
            del self.connection[connectionID]
        except:
            pass 
 
    def get_cursor(self, connectionID = None, cursorID = None):
        """
        _get_cursor_
    
        
        input: connectionID (string)
    
        output: cursor object
      
        Returns a low level cursor object to get low level control over the 
        database interactions.
        """
        if connectionID == None:
            connectionID = self.currentConnection
        if cursorID == None:
            cursorID = self.currentCursor
    
        if not self.connection.has_key(connectionID):
            raise ProdException(exceptions[4002], 4002)
        if not self.connection[connectionID]['cursors'].has_key(cursorID):
            raise ProdException(exceptions[4013], 4013)
    
        return self.connection[connectionID]['cursors'][cursorID]['cursor']
 

