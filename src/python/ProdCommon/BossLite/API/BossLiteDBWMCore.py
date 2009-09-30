#!/usr/bin/env python
"""
_BossLiteDBWMCore_

"""

__version__ = "$Id: BossLiteDBWMCore.py,v 1.1 2009/08/03 10:00:26 gcodispo Exp $"
__revision__ = "$Revision: 1.1 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging
from os.path import expandvars
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter

##########################################################################

class BossLiteDBWMCore(object):
    """
    High level API class for DB queries through WMCore.
    It allows load/operate/update DB using free format queries
    """

    dbConfig =  {'dialect': '???',
                 'user': '???',
                 'username': '???',
                 'passwd': '???',
                 'password': '???',
                 'tnsName': '???',
                 'host' : '???',
                 'port' : '???',
                 'sid' : '???'
                 }

    def __init__(self, database, dbConfig):
        """
        initialize the API instance
        """
        
        # get logger
        self.logger = logging.getLogger()

        # create an instance of database
        if isinstance(dbConfig, basestring):
            self.dbInstance = DBFactory(self.logger, dburl=dbConfig)
            self.dbConfig = dbConfig
        else:
            self.dbInstance = DBFactory(self.logger, options=dbConfig)
            self.dbConfig.update( dbConfig )

        # report error if not successful
        if self.dbInstance is None:
            self.logger.error( "Failed to Initialize BossLiteDBWMCore" )
            return
        
        # create a session and db access
        self.session = None


    ##########################################################################
    def connect ( self ) :
        """
        recreate a session and db access
        """

        # create a session and db access
        if self.session is None:
            self.session = self.dbInstance.connect()


    ##########################################################################
    def close ( self ) :
        """
        close session and db access
        """
        
        # Does "close" method exist for SQLAlchemy? Not present in DBFactory ...
        self.session.close()
        self.session = None


    ##########################################################################
    def reset ( self ) :
        """
        reset session and db access
        """

        self.close()
        self.connect()


    ##########################################################################
    def commit ( self ) :
        """
        commit
        """
        
        # empty method
        pass

    ##########################################################################
    def select(self, query):
        """
        execute a query.
        """

        # db connect
        self.session.connect()

        # -> WMCore.Database.ResultSet import ResultSet
        results = self.session.processData(query)
    
        if (results.rowcount > 0):
            formatter = DBFormatter(self.logger, self.session)
            out = formatter.format(results)
        else :
            out = None

        return out


    ##########################################################################
    def selectOne(self, query):
        """
        execute a query.with only one result expected
        """

        # db connect
        self.session.connect()

        # execute query
        results = self.session.processData(query)
    
        if (results.rowcount > 0):
            formatter = DBFormatter(self.logger, self.session)
            out = formatter.formatOne(results)
        else :
            out = None

        return out


    ##########################################################################
    def modify(self, query):
        """
        execute a query which does not return such as insert/update/delete
        """

        # db connect
        self.connect()

        # return query results.... 
        self.session.processData(query)


    ##########################################################################
    def updateDB( self, obj ) :
        """
        update any object table in the DB
        works for tasks, jobs, runningJobs
        """

        # db connect
        self.connect()

        # update
        obj.update(self.session)


    ##########################################################################
    def installDB( self, schemaLocation = None ) :
        """
        install database
        """
        # ...
        if schemaLocation is not None:
            schemaLocation = expandvars( schemaLocation )
            self.dbConfig.update({ 'host' : schemaLocation})
        
        daofactory = DAOFactory(package = "WMCore.Services", 
                                logger = self.logger, 
                                dbInterface = self.session)
        
        mydao = daofactory(classname = "BossLite." + self.dbConfig['dialect'] 
                           +".Create")
        status = mydao.execute()
        
        # check creation...
        return status


    ##########################################################################
    
