#!/usr/bin/env python

"""
_Connection_
Connection class for accessing the database backends and formating the results fetched from the database   
This Wrapper class integrates the sqlalchemy functionality at the backend to deal with connection management and connection pooling. It provides interface to the operation class to connect to multiple databases at a time and provides the functionality to use one connection pool across multiple threads. 

The main objective was to separate out the connection part from the operations part. It allows operation class to connect tomultiple databases at a time.  

"""

import base64
import cPickle
import logging
import threading
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Core.Codes import exceptions
from ProdAgentDB.Config import loadConfig
import time
try:
   from sqlalchemy.engine import create_engine
   from sqlalchemy.orm import sessionmaker
   from sqlalchemy.engine.url import URL
   from sqlalchemy.exceptions import *

except:
   raise RuntimeError(1,"sqlalchemy could not be found. Make sure it is "+  \
                         "installed. For further details look at " + \
                         "http://www.sqlalchemy.org/docs/04/intro.html")


class Connection:

    """
    Connection object to connect to multiple databases. It is NOT THREAD-SAFE. But it allows you to connect to multiple
    databases. But One object will talk to only one database. Inorder to talk to multiple databases, you need to 
    instantiate multiple objects.
    """

    #  //
    # // Dictionary containing the engine instances shared amoung multiple threads
    #//
    databaseEngines = None
    
    #// Dictionary containing the session instances local to current thread
    sessionCollection = None


    def __init__ (self,**connection_parameters):

       """
       _init_ method that accepts connection parameters to initialize instance attributes and assigns connection from 
        connection pool to the object calling it. 

        connection parameters includes:
        
        dbType              : Type of database i.e mysql, oracle
        dbName              : database name
        host                : host (if local use "localhost")
        user                : user name
        passwd              : password
        socketFileLocation  : socket file location (use if connect local)
                              but leave empty if connecting from remote.
        portNr              : port number if you connect from remote.
                              Leave empty if you connect via a socket file.
        pool_size           : No. of connections to keep open inside the connection pool
        pool_recycle        : This setting cause the pool to recycle connection after given # of seconds has passed
        pool_timeout        : No. of seconds to wait before giving up to get connection from the pool 
        max_overflow        : No. of connection that can be opened above and beyond the pool_size connection limit. But 
                              these connection will not return to the pool after close call. They will rather be discarded.                               If set to -1 then infinite number of connections can be opened. 

       """
       
       #  //
       # // Make sure that user provide minimum required parameters ['dbType', 'dbName', 'user', 'passwd', 'host']       
       #//
       if not connection_parameters.has_key('dbType'):
          connection_parameters['dbType'] = 'mysql'

       self.argsValidityCheck(**connection_parameters)
                
       #  //
       # // Assigning default values
       #//
 
       self.connection_parameters = loadConfig('ProdAgentDB')
       self.connection_parameters['dbType'] = 'mysql'
       self.connection_parameters['socketFileLocation'] = ''
       self.connection_parameters['portNr'] = ''
       self.connection_parameters['pool_size'] = 50
       self.connection_parameters['pool_recycle'] = -1 
       self.connection_parameters['max_overflow'] = -1
       self.connection_parameters['pool_timeout'] = 30 
       self.connection_parameters['echo'] = False 
       self.connection_parameters['echo_pool'] =  False
       self.connection_parameters['maxConnectionAttempts'] = 5
       self.connection_parameters['dbWaitingTime'] = 10
       self.connection_parameters.update(**connection_parameters)    

       if self.connection_parameters['echo'] == 'True':
          self.connection_parameters['echo'] = True

       elif self.connection_parameters['echo'] == 'False':
         self.connection_parameters['echo'] = False


       if self.connection_parameters['echo_pool'] == 'True':
          self.connection_parameters['echo_pool'] = True

       elif self.connection_parameters['echo_pool'] == 'False':
          self.connection_parameters['echo_pool'] = False
                

       #  //
       # // Instance attributes
       #// 
       self.engine = None
       self.session = None
       self.queryCollection = None 
       self.resultProxy = None  
       
       #// Instantiate quries attribute correspond to particular instance
 
       if self.queryCollection is None:
          self.queryCollection = []        

       #// Instantiate threadlocal session collection
       
       if self.__class__.sessionCollection is None: 
          self.__class__.sessionCollection = threading.local()
        
       if len(self.__class__.sessionCollection.__dict__) == 0: 
          self.__class__.sessionCollection.__dict__['sessions'] = []

       
       #  //
       # // Retrieving engine instance corrresponds to the connection parameters provided
       #//      

       if self.__class__.databaseEngines is None:
             self.__class__.databaseEngines = {}
       
       self.retrieveEngine ()

       #  //
       # // Maintain the session dictionary that open in current thread
       #//
 
       self.addSession ()     


       return    #// init method

    def __del__ (self):
       
        try: 
           self.close()
        except:
           return
        


    def getConnectionParameters (self):

       """
       Input: None
       Outpur: Returns the connection parameters attached to this object

       """

       return self.connection_parameters

    def engineDictionary (self):

       """
       Input: None
       Output: Return the Engine dictionary 
       """

       return self.__class__.databaseEngines


    def sessionDictionary (self):

       """
       Input: None
       Output: Return the Dictionary of sessions that are opened in current thread  
       """  
       return self.__class__.sessionCollection.__dict__

    def addSession (self):
 
       """
       Input : None
       Output: None

       Appends the newly allocated session to the threadlocal session dictionary

       """

       flag = False
       for sess in self.__class__.sessionCollection.__dict__['sessions']:
          if sess['sessionId'] is self.session:
             flag = True
             break

       if flag is False:
          temp = {}
          temp['sessionId'] = self.session
          temp['quries'] = []
          self.__class__.sessionCollection.__dict__['sessions'].append(temp)

       return

	
    def argsValidityCheck(self, **connection_parameters):

       """
       Function that ensures that user provided min required  connection parameters  

       """
       minRequired = ['dbName', 'host', 'user', 'passwd', 'dbType', 'socketFileLocation', 'portNr']
      
       #  //
       # // Raises exception if the provided connection parameters are missing any minRequired parameter list element 
       #//

       if connection_parameters['dbType'] == 'mysql':
        
        for arg in minRequired:

          if not (arg in connection_parameters.keys()):

             if  arg not in ['socketFileLocation','portNr']:
                logging.error ('Parameter: '+ arg + ' NOT provided') 
                raise ProdException(exceptions[4012], 4012)
             else:
                connection_parameters[arg] = ""    
  
        #// Raises exception if both socketFileLocation and portNr are provided non empty Or if none of them is given.        
       
        if (connection_parameters['socketFileLocation'] == "" and connection_parameters['portNr'] == "") or \
           (connection_parameters['socketFileLocation'] != "" and connection_parameters['portNr'] != ""):

          logging.error("Either Both socketFileLocation and portNr are empty OR Both are Non empty. Please provide only \
                         one")                       
          raise ProdException(exceptions[4012], 4012)


       elif connection_parameters['dbType'] == 'oracle':
          
          minRequired = ['user', 'passwd', 'dbType','tnsName']

          for arg in minRequired:
             if not (connection_parameters.has_key(arg)):
                msg = 'Missing Parameter: %s' %arg
                
                logging.error(msg)
                raise ProdException(exceptions[4012], 4012)
         

       return  #//argsValidityCheck


                 
    def retrieveEngine (self):
    
       """
       Function that instantiate and returns the engine instance based on the tripplet ['dbName, 'dbType', 'host'] if it
       is not already present in the databaseEngines dictionary  
       """       

       #  //
       # // Make url based on the connection parameters provided
       #//
       BindSession = None 
       url = self.getUrl()       
       cache = False
       
       #  //
       # // Checks whether engine instance corresponds to the provided connection parameters already exist or not. If NOT
       #//  then instantiate new one and add it to the databaseEngines dictionary   
         
       for engineUrl in self.__class__.databaseEngines.keys():
          
          
          if url.__eq__(engineUrl) :

             self.engine = self.__class__.databaseEngines[engineUrl]['engineInstance']               
             BindSession = self.__class__.databaseEngines[engineUrl]['bindedSessionClass']                       
             self.session = BindSession()                       
             cache = True
             return  

               
       if  cache is False:

          
          try:

             
             if self.connection_parameters['dbType'] == 'mysql':
              
              #// If connection via socket file to local server
              if self.connection_parameters['socketFileLocation'] != '' :

                self.engine = create_engine(url, connect_args = {'unix_socket':\
                                            self.connection_parameters['socketFileLocation']}, strategy= 'plain', \
                                            pool_size =    int(self.connection_parameters['pool_size']),\
                                            max_overflow = int(self.connection_parameters['max_overflow']),\
                                            pool_timeout = int(self.connection_parameters['pool_timeout']), \
                                            pool_recycle = int(self.connection_parameters['pool_recycle']), \
                                            echo = self.connection_parameters['echo'], \
                                            echo_pool= self.connection_parameters['echo_pool'])

              #// If connecting via port to remote server
              else:

                self.engine = create_engine(url , strategy= 'plain', \
                                            pool_size =  int(self.connection_parameters['pool_size']), \
                                            max_overflow = int(self.connection_parameters['max_overflow']), \
                                            pool_timeout = int(self.connection_parameters['pool_timeout']), \
                                            pool_recycle = int(self.connection_parameters['pool_recycle']), \
                                            echo = self.connection_parameters['echo'], \
                                            echo_pool=  self.connection_parameters['echo_pool'])

             elif self.connection_parameters['dbType'] == 'oracle':
                
                self.engine = create_engine(url , strategy= 'plain', \
                                            pool_size =  int(self.connection_parameters['pool_size']), \
                                            max_overflow = int(self.connection_parameters['max_overflow']), \
                                            pool_timeout = int(self.connection_parameters['pool_timeout']), \
                                            pool_recycle = int(self.connection_parameters['pool_recycle']), \
                                            echo = self.connection_parameters['echo'], \
                                            echo_pool=  self.connection_parameters['echo_pool'])


             #// session maker returns a class that is binded with this engine. We can make session instances from 
             #// this class
             self.engine.__dict__['pool'].__dict__['_use_threadlocal'] = False #True
             BindSession = sessionmaker (bind = self.engine, transactional = True)

             #// Append newly created engine instance to the engine dictionary

             self.__class__.databaseEngines[url] = {}
             self.__class__.databaseEngines[url]['engineInstance'] = self.engine
             self.__class__.databaseEngines[url]['bindedSessionClass'] = BindSession
             self.session = BindSession()   


          except Exception, ex:

             logging.error("Unable to create Engine based on the connection parameters provided")         
             
             raise ProdException(exceptions[4013], 4013)                   
          
       return #//retrieveEngine                            
              
                  
    def getUrl (self):
    
       """
       Make and returns the connection URL corresponds to the connection parameters provided
       Attributes on URL include:

      drivername     : The name of the database backend. This name will correspond to a module in sqlalchemy/databases or 
                       a third party plug-in.
       username       : The user name for the connection.
       password       : database password.
       host           : The name of the host.
       port           : The port number.
       database       : The database.
       """
       
       connectionUrl = None

       if self.connection_parameters['dbType'] == 'mysql':

        if self.connection_parameters['portNr'] == '' :
          connectionUrl = URL (drivername = self.connection_parameters['dbType'],\
                               username = self.connection_parameters['user'],\
                               password = self.connection_parameters['passwd'],\
                               host = self.connection_parameters['host'],\
                               database = self.connection_parameters['dbName'])
        else:
           connectionUrl = URL (drivername = self.connection_parameters['dbType'],\
                               username = self.connection_parameters['user'],\
                               password = self.connection_parameters['passwd'],\
                               host = self.connection_parameters['host'],\
                               port = int(self.connection_parameters['portNr']),\
                               database = self.connection_parameters['dbName'])
       
       elif self.connection_parameters['dbType'] == 'oracle':
       
           #  //
           # // Connecting via TNS Name
           #//

           connectionUrl = URL (drivername = self.connection_parameters['dbType'],\
                               username = self.connection_parameters['user'],\
                               password = self.connection_parameters['passwd'],\
                               host = self.connection_parameters['tnsName'])
 
       else:
           
          raise RuntimeError(2,"Incompatible dbType provided ")   

       return connectionUrl   #//getUrl  
       

    def commit (self):

       """
       _commit_

       Input :  None
       Output : None
 
       Commits the transaction for a particular connection (connection)  
       """   
       
       if self.session != None:

          self.execute('COMMIT')
          self.session.commit()
          self.queryCollection = [] 
  
          logging.debug("Transaction Committed")                
   
       return #// commit

    def rollback (self):

       """
       _rollback_
      
       Input : None
       Output : None

       Rollsback a particular transaction associated to a connection

       """

       if self.session != None:

          self.execute('ROLLBACK')
          self.session.rollback()
          self.queryCollection = [] 

          logging.debug("Transaction rolled back")
          


       return #// rollback

    def execute (self, sqlQuery, bindParams = None):

       """
       _execute_

       Input : sql query
       Output

       Function that actually executes the provided sql query using the Instance attribute 'session'
       and if the query fails due to connection loss then it try to restore connection and redo all queries
       since last commit     

       """
       
       if self.session is None:
          logging.debug("No connection found, Object already been closed")
          raise ProdException(exceptions[4003], 4003) 

       try:        
         
           
          self.resultProxy = self.session.execute(sqlQuery, bindParams)
          
          cursor = self.resultProxy.cursor
          rowsModified = cursor.rowcount
          self.queryCollection.append([sqlQuery,bindParams])

          return rowsModified               
   

       except Exception, ex:
          msg = "Connection to the database lost, Problem: %s" % (str(ex))          
          logging.warning (msg)

          #// Closing the session to make connection pool persistant
          self.refreshSession()
        
          #// Redo ALL quries since last commit. On new execution session will automatically pick up valid connection
          #// from the underlying connection pool
          self.redo()

          #// Redo the last query which hasn't been cached into queryCollection instance
          
          self.resultProxy = self.session.execute(sqlQuery, bindParams)
          cursor = self.resultProxy.cursor
          rowsModified = cursor.rowcount

          self.queryCollection.append ([sqlQuery, bindParams])
          return rowsModified          
       
    def  refreshSession(self):

       """
       Input : None
       Output : None

       Refresh the session state after connection lose
       """ 
       
       connected = False
        
       try:

          if not self.session.connection().__dict__.has_key('_Connection__connection'):
             self.session.close() 
              
          
          elif self.session.connection().__dict__['_Connection__connection'].__dict__['connection'] == None :              
             self.session.close() 
             
   
          elif self.session.connection().__dict__['_Connection__connection'].__dict__['connection'] != None :            

             self.session.connection().invalidate()
             self.session.rollback()
             self.session.close()
             
          else :              
             return

       except (OperationalError, DatabaseError), ex:
       
          msg = 'SERVER WENT DOWN : ' + str(ex)          
          logging.warning(msg)
             
          for attempt in range(int(self.connection_parameters['maxConnectionAttempts'])):
             try:

                self.session.connection()

             except (OperationalError, DatabaseError), e:
                msg = 'SERVER WENT DOWN : ' + str(e)
                msg += '\n'
                msg += 'Trying to reconnect : ' + str(attempt)
                
                logging.warning(msg)
                time.sleep(int(self.connection_parameters['dbWaitingTime'])) 
                connected = True 

             else:                      
               
                return


 
       if not connected:
          
          for attempt in range(int(self.connection_parameters['maxConnectionAttempts'])):

             try:
 
                self.session.connection()

             except (OperationalError, DatabaseError), ex:
 
                msg = 'SERVER WENT DOWN : ' + str(ex)
                msg += '\n'
                msg += 'Trying to reconnect : ' + str(attempt)
                
                logging.warning(msg)
                time.sleep(int(self.connection_parameters['dbWaitingTime']))

             else:
                                 
                connected = True                   
                return
              
           
       raise ProdException(exceptions[4007],4007)
       
       return
  
    def redo (self):

       """
       Redo all quries that belongs to current connection since last commit. It tries redo operation only once. If it 
       again gets fail then raises exception  

       """

       
       try:
           
          if (self.session != None):
             
             logging.debug('Redo all quries since last COMMIT')             
             
             for query in self.queryCollection:
                   
                self.session.execute(query[0],query[1]) 

       except Exception,ex:
          
          msg = "Connection to the database lost, Problem: %s" % (str(ex))
          msg += '\n'
          msg += 'Redo Operation also failed. Aborting transaction'
          
          
          #// closing session to maintain pool consistancy 
          
          self.refreshSession()
          logging.warning (msg)
           
          #// rasing exception:: Error connecting to database
          raise ProdException(exceptions[4006], 4006)            

       return #// redo


       

    def close (self):

       """
       _close_

       input: nothing

       output: nothing

       Closes the connection

       """
       
       
       if self.resultProxy is not None:
          self.resultProxy.close()
          self.resultProxy = None

       if self.session != None:

          self.session.close()
          self.queryCollection = []

          #// Removing this session from session collection
          for sess in self.__class__.sessionCollection.__dict__['sessions']:
             if sess['sessionId'] is self.session:
                self.__class__.sessionCollection.__dict__['sessions'].remove(sess)

          self.session = None  
 

       return #// close 

    def get_cursor (self):

       """
       _get_cursor_


       input: None

       output: cursor object

       Returns a low level cursor object to get low level control over the
       database interactions.
       """

       if self.resultProxy != None:

          return self.resultProxy.cursor

       else:
          logging.error ("No open transaction found")          
          raise ProdException(exceptions[4003],4003)


       


    def fetchall (self, dictionary = False, decode = [], decode64 = []):

       """
       _fetchall_


        input: (Optional)

               dictionary: If required output in dictionary format
               decode: If wants to apply base64 decoding and de-serialization on certain attributes
               decode64: If only base64 decoding is required but de-serialization not required on certain attributes 
                        

        output: list of database rows that are the results form the last query.        
       """

       cursor = self.get_cursor()
       if dictionary :
          return convert(cursor.fetchall(), False, decode, decode64)


       return cursor.fetchall()
       

    def fetchone (self, dictionary = False, decode = [], decode64 = []):

       """
       _fetchone_

       input: (Optional)

               dictionary: If required output in dictionary format
               decode: If wants to apply base64 decoding and de-serialization on certain attributes
               decode64: If only base64 decoding is required but de-serialization not required on certain attributes
 
 
       output: one database row that is the results form the last query.
       """

       cursor = self.get_cursor()

       if dictionary :
          return convert([cursor.fetchone()], True, decode, decode64)

       return cursor.fetchone()
       

    def commit_all (self):

       """
       NOT IMPLEMENTED YET 

       """

       return #// commit_all

    def close_all (self):

       """
       NOT IMPLEMENTED YET
 
       """

       return #// close_all 

    def rollback_all (self):

       """
       NOT IMPLEMENTED YET 

       """

       return #// rollback_all

    def convert (self, description = None , rows = [], oneItem = False, decode = [], decode64 = []):

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

       if len(description) == 0:
          cursor = self.get_cursor()
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
       
    def poolStatus (self):

       """
       Input  : Nothing
       Output : Return the current pool status
       """
      
       if self.engine != None:

          return self.engine.__dict__['pool'].status() + ' CheckedIn: ' + str(self.engine.__dict__['pool'].checkedin())

       else:
      
          return None

    def connectionStatus (self):

       """
       Input : Nothing
       Output : Return the Actual DB connection status that is being holded by current Operation Instance

     

       """

       if self.session != None:
          
          return str(self.session.connection().__dict__['_Connection__connection'].__dict__['connection'])  

       else:
  
          return None #// status
