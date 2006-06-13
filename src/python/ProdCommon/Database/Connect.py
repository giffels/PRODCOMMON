#!/usr/bin/env python
from ProdCommon.Database.Config import defaultConfig
from ProdCommon.Database import Mysql

def connect(**config):
     """
     _connect_  Connects to the underlying MySQL database.
    
     If a set of attributes is passed with the connect method,
     it has to have 6 attributes:

     dbName              : database name
     host                : host (if local use "localhost")
     user                : user name
     passwd              : pass word
     socketFileLocation  : socket file location (use if connect local)
                           but leave empty if connecting from remote.
     portNr            : port number if you connect from remote.
                           Leave empty if you connect via a socket file.
     """
     actualConfig = defaultConfig
     if config != {}:
          actualConfig = config
     try:
         conn=Mysql.connect(actualConfig['dbName'],\
                            actualConfig['host'],\
                            actualConfig['user'],\
                            actualConfig['passwd'],\
                            actualConfig['socketFileLocation'],\
                            actualConfig['portNr'])
         return conn
     except:
         raise

