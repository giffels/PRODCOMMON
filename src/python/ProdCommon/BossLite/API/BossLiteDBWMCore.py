#!/usr/bin/env python
"""
_BossLiteDB_

"""

__version__ = "$Id: BossLiteDBWMCore.py,v 1.0 2008/10/10 13:32:37 gcodispo Exp $"
__revision__ = "$Revision: 1.0 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging

# Database imports:
# import WMCore stuff, this is just an example...
from WMCore.Database.DBCreator import DBCreator
from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.JobStateMachine.ChangeState import Transitions



##########################################################################

class BossLiteDBWMCore(object):
    """
    High level API class for DB queries through WMCore.
    It allows load/operate/update DB using free format queries

    """

    # this must be adapted to WMCore standard configuration parameters
    dbConfig =  {'dbName':'BossLiteDB',
                 'user':'BossLiteUser',
                 'passwd':'BossLitePass',
                 'socketFileLocation':'',
                 'host':'',
                 'portNr':'',
                  'refreshPeriod' : 4*3600 ,
                 'maxConnectionAttempts' : 5,
                 'dbWaitingTime' : 10
                 }

    def __init__(self, database, dbConfig):
        """
        initialize the API instance
        - database should be WMCore
        - dbConfig should have a proper form to initialize WMCore DB access

        """

        # database
        self.database = database       # "MySQL" or "SQLite"

        # update db config
        self.dbConfig.update( dbConfig )

        # create a session and db access
        self.session = None


    ##########################################################################
    def connect ( self ) :
        """
        recreate a session and db access
        """

        # create a session and db access
        if self.session is None:
            self.session = None

        # WARNING!!!!
        # Here is important having a self.session object of some kind


    ##########################################################################
    def close ( self ) :
        """
        close session and db access
        """

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

        self.session.commit()


    ##########################################################################
    def select(self, query):
        """
        execute a query.
        """

        # db connect
        self.connect()

        if (self.session.execute(query) > 0):
            out = self.session.fetchall()
        else :
            out = None

        # return query results
        return out


    ##########################################################################
    def selectOne(self, query):
        """
        execute a query.with only one result expected
        """

        # db connect
        self.connect()

        if (self.session.execute(query) > 0):
            out = self.session.fetchone()[0]
        else :
            out = None

        # return query results
        return out


    ##########################################################################
    def modify(self, query):
        """
        execute a query which does not return such as insert/update/delete
        """

        # db connect
        self.connect()

        # return query results
        self.session.execute( query )
        self.session.commit()


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
        self.session.commit()


    ##########################################################################
    def installDB( self, schemaLocation ) :
        """
        install database
        """
        raise NotImplementedError

    ##########################################################################





