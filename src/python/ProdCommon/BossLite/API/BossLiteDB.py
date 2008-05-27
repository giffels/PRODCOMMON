#!/usr/bin/env python
"""
_BossLiteAPI_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging
from os.path import expandvars

# Database imports
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.BossLite.Common.Exceptions import DbError


##########################################################################

class BossLiteDB(object):
    """
    High level API class for DB queries.
    It allows load/operate/update DB using free format queries

    """

    def __init__(self, database, dbConfig):
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

        # MySQL: get DB configuration from config file
        if self.database == "MySQL":
            # update db config
            self.dbConfig =  {'dbName':'BossLiteDB',
                              'user':'BossLiteUser',
                              'passwd':'BossLitePass',
                              'socketFileLocation':'',
                              'host':'',
                              'portNr':'',
                              'refreshPeriod' : 4*3600 ,
                              'maxConnectionAttempts' : 5,
                              'dbWaitingTime' : 10
                              }
            dbConfig['socketFileLocation'] = expandvars(
                dbConfig['socketFileLocation']
                )
            self.dbConfig.update( dbConfig )

            # create DB instance
            from ProdCommon.Database.MysqlInstance import MysqlInstance
            self.dbInstance = MysqlInstance(self.dbConfig)

        else:
            # update db config
            self.dbConfig =  {'dbName':'BossLiteDB'}
            dbConfig['dbName'] = expandvars( dbConfig['dbName'] )
            self.dbConfig.update( dbConfig )

            # create DB instance
            from ProdCommon.Database.SqliteInstance import SqliteInstance
            self.dbInstance = SqliteInstance(self.dbConfig)

        # create a session and db access
        self.session = None


    ##########################################################################
    def connect ( self ) :
        """
        recreate a session and db access
        """

        # create a session and db access
        if self.session is None:
            self.session = SafeSession(dbInstance = self.dbInstance)


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

        schemaLocation = expandvars( schemaLocation )

        if self.database == "MySQL":
            self.installMySQL( schemaLocation )

        elif self.database == "SQLite":
            self.installSQlite( schemaLocation )

        else:
            raise NotImplementedError


    ##########################################################################
    def installMySQL( self, schemaLocation ) :
        """
        install MySQL database
        """
        import getpass
        from ProdCommon.Database.MysqlInstance import MysqlInstance

        # ask for password (optional)
        print
        userName = raw_input(
"""
Please provide the mysql user name (typically "root") for updating the
database server (leave empty if not needed): ')
""" )

        if userName == '' :
            userName = 'root'
            print

        passwd = getpass.getpass(
"""Please provide mysql passwd associated to this user name for
updating the database server:
""" )

        # define connection type
        from copy import deepcopy
        rootConfig = deepcopy( self.dbConfig )
        rootConfig.update(
            { 'dbName' : 'mysql', 'user' : userName, 'passwd' : passwd }
            )
        dbInstance = MysqlInstance( rootConfig )
        session = SafeSession( dbInstance = dbInstance )

        # check if db exists
        create = True
        query = "show databases like '" + self.dbConfig['dbName'] + "'"
        try:
            session.execute( query )
            session.commit()
            results = session.fetchall()
            if results[0][0] == self.dbConfig['dbName'] :
                print "DB ", self.dbConfig['dbName'], "already exists.\n"
                create = False
        except IndexError :
            pass
        except Exception, msg:
            session.close()
            raise DbError(str(msg))

        # create db
        if create :
            query = 'create database ' + self.dbConfig['dbName']
            try:
                session.execute( query )
                session.commit()
            except Exception, msg:
                session.close()
                raise DbError(str(msg))

        # create tables
        queries = open(schemaLocation).read()
        try:
            session.execute( 'use ' + self.dbConfig['dbName'] )
            session.commit()
            for query in queries.split(';') :
                if query.strip() == '':
                    continue
                session.execute( query )
                session.commit()
        except Exception, msg:
            session.close()
            raise DbError(str(msg))

        # grant user
        query = 'GRANT UPDATE,SELECT,DELETE,INSERT ON ' + \
                self.dbConfig['dbName'] + '.* TO \'' + \
                self.dbConfig['user'] + '\'@\'' + self.dbConfig['host'] \
                + '\' IDENTIFIED BY \'' + self.dbConfig['passwd'] + '\';'
        try:
            session.execute( query )
            session.commit()
        except Exception, msg:
            session.close()
            raise DbError(str(msg))

        # close session
        session.close()


    ##########################################################################
    def installSQlite( self, schemaLocation ) :
        """
        install SQLite database
        """

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)

        # execute check query
        query = "select tbl_name from sqlite_master where tbl_name='bl_task'"

        try:
            # if bl_task exists, no further operations are needed
            session.execute( query )
            results = session.fetchall()
            if results[0][0] == self.dbConfig['dbName'] :
                print "DB ", self.dbConfig['dbName'], "already exists.\n"
                return
            session.close()
            return
        except IndexError :
            pass
        except StandardError:
            pass

        try:
            # if bl_task exists, no further operations are needed
            session.execute("select count(*) from bl_task")
            session.close()
            return
        except StandardError:
            pass


        # execute query
        queries = open(schemaLocation).read()
        try:
            for query in queries.split(';') :
                session.execute(query)
        except Exception, msg:
            raise DbError(str(msg))

        # close session
        session.close()


    ##########################################################################




