#!/usr/bin/env python
"""
_BossLitePoolDB_

"""

__version__ = "$Id: BossLitePoolDB.py,v 1.0 2008/07/11 15:41:45 gcodispo Exp $"
__revision__ = "$Revision: 1.0 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging
from os.path import expandvars

# Database imports
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.Database.SafePool import SafePool
from ProdCommon.BossLite.API.BossLiteDB import BossLiteDB
from ProdCommon.BossLite.Common.Exceptions import BossLiteError


##########################################################################

class BossLitePoolDB(BossLiteDB):
    """
    High level API class for DB queries using SafePool.
    It allows load/operate/update DB using free format queries

    """


    def __init__(self, database, pool=None, dbConfig=None):
        """
        initialize the API instance
        - database can be both MySQl or SQLite

        - dbConfig can be a dictionary with the format
           {'dbName':'BossLiteDB',
               'host':'localhost',
               'user':'BossLiteUser',
               'passwd':'BossLitePass',
               'socketFileLocation':'/var/run/mysql/mysql.sock',
               'portNr':'',
               'refreshPeriod' : 4*3600 ,
               'maxConnectionAttempts' : 5,
               'dbWaitingTime' : 10
              }

        """

        # database
        self.database = database       # "MySQL" or "SQLite"

        # pool
        self.pool = pool

        # MySQL: get DB configuration from config file
        if self.database != "MySQL":
            raise BossLiteError( 'invalid database type' )

        if pool is None and dbConfig is not None :
            
            # update db configdbConfig
            self.dbConfig['socketFileLocation'] = expandvars(
                self.dbConfig['socketFileLocation']
                )
            self.dbConfig.update( dbConfig )

            # create DB instance
            from ProdCommon.Database.MysqlInstance import MysqlInstance
            self.dbInstance = MysqlInstance(self.dbConfig)
            self.pool = SafePool(self.dbInstance, 3)

            
        # create a session and db access
        self.session = None


    ##########################################################################
    def connect ( self ) :
        """
        recreate a session and db access
        """

        # create a session and db access
        if self.session is None:
            self.session = SafeSession(pool = self.pool)


    ##########################################################################
    def getPool ( self ) :
        """
        returns SafePool for therad usage
        """

        return self.pool

