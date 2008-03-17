#!/usr/bin/env python
"""
_BossLiteAPI_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging

# Database imports
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.BossLite.DbObjects.TrackingDB import TrackingDB

try:
    from ProdCommon.Database.MysqlInstance import MysqlInstance
except ImportError :
    print "Warning: missing MySQL\n"

try:
    from ProdCommon.Database.SqliteInstance import SqliteInstance
except ImportError :
    print "Warning: missing pysqlite2\n"
    
# Task and job objects
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
from ProdCommon.BossLite.Common.Exceptions import *

from os.path import expandvars
import copy

##########################################################################

class BossLiteAPI(object):
    """
    High level API class for DBObjets.
    It allows load/operate/update jobs and taks using just id ranges
    
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
            self.dbInstance = MysqlInstance(self.dbConfig)

        else:
            # update db config
            self.dbConfig =  {'dbName':'BossLiteDB'}
            dbConfig['dbName'] = expandvars( dbConfig['dbName'] )
            self.dbConfig.update( dbConfig )

            # create DB instance
            self.dbInstance = SqliteInstance(self.dbConfig)

        # create a session and db access
        self.session = SafeSession(dbInstance = self.dbInstance)
        self.db = TrackingDB(self.session)


    ##########################################################################
    def connect ( self ) :
        """
        recreate a session and db access
        """

        # create a session and db access
        self.session = SafeSession(dbInstance = self.dbInstance)
        self.db = TrackingDB(self.session)
        

    ##########################################################################
    def close ( self ) :
        """
        close session and db access
        """

        self.session.close()
        self.db = None

    ##########################################################################
        
    def declare( self, xml, proxyFile=None ) :
        """
        register job related informations in the db
        """

        taskInfo, jobInfos, rjAttrs = self.deserialize( xml )
       
        # reconstruct task
        task = Task( {'name' : taskInfo['name'], 'user_proxy' : proxyFile} ) 
        task.updateInternalData()
        self.saveTask( task )
        for key in taskInfo:
            task[key] = taskInfo[key] 
        self.updateDB( task )
        
        # reconstruct jobs and fill the data
        jobs = []
        for jI in jobInfos:
            job = Job( jI )
            jobs.append(job)

        task.addJobs(jobs)
        self.updateDB( task )
        del jobs

        # reconstruct running jobs
        task = self.loadTaskByName(taskInfo['name'])
        
        for i in xrange(len(task.jobs)):
            attrs = rjAttrs[ str(task.jobs[i]['name']) ]
            self.getRunningInstance(task.jobs[i]) 

            for key in attrs:
                if not attrs[key] or attrs[key] == 'None':
                    continue 
                task.jobs[i].runningJob[key] = str(attrs[key])
        self.updateDB( task )

        # all done
        return self.loadTaskByName(taskInfo['name'])


    ##########################################################################

    def saveTask( self, task ):
        """
        register task related informations in the db
        """

        # db connect
        if self.db is None :
            self.connect()

        # save task
        task.updateInternalData()
        task.save(self.db)
        self.session.commit()

        return task

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

        
    def updateDB( self, obj ) :
        """
        update any object table in the DB
        works for tasks, jobs, runningJobs
        """

        # db connect
        if self.db is None :
            self.connect()

        # update
        obj.update(self.db)
        self.session.commit()


    ##########################################################################

        
    def loadTask( self, taskId, jobAttributes=None ) :
        """
        retrieve task information from db using task id
        and defined jobAttributes
        """

        # db connect
        if self.db is None :
            self.connect()

        # defining default
        if jobAttributes is None:
            jobAttributes = {}

        # create template for task
        task = Task()
        task['id'] = taskId

        # create template for jobs with particular jobAttributes
        job = Job(jobAttributes)
        task.addJob(job)

        # load task
        task.load(self.db)
        
        return task

    ##########################################################################
    ###### Added By DanieleS. 
    def loadTaskByID( self, ID ) :
        """
        retrieve task information from db for task 'name'
        """

        # db connect
        if self.db is None :
            self.connect()

        # create template for task
        task = Task()
        task['id'] = ID

        # load task
        task.load(self.db)
        
        return task
        
    def loadTaskByName( self, name ) :
        """
        retrieve task information from db for task 'name'
        """

        # db connect
        if self.db is None :
            self.connect()

        # create template for task
        task = Task()
        task['name'] = name

        # load task
        task.load(self.db)
        
        return task

    ##########################################################################

        
    def loadTasksByUser( self, user ) :
        """
        retrieve task information from db for task owned by user
        """

        # db connect
        if self.db is None :
            self.connect()

        # create template for task
        task = Task()
        task['user'] = user

        # load task
        taskList = self.db.select(task)
        
        return taskList
  
    ##########################################################################

        
    def loadTasksByProxy( self, name ) :
        """
        retrieve task information from db for all tasks
        with user proxy set to name
        """

        # db connect
        if self.db is None :
            self.connect()

        # create template for task
        task = Task()
        task['user_proxy'] = name

        # load task
        taskList = self.db.select(task)
        
        return taskList

    ##########################################################################

        
    def load( self, taskRange, jobRange="all", jobAttributes=None ) :
        """
        retrieve information from db for:
        - range of tasks
        - range of jobs inside a task
        - various job attributes (logic and)

        In some way these shuold be the option to build the query.
        Maybe, same options should be used also in
        loadSubmitted, loadCreated, loadEnded, loadFailed
         
        Takes the highest submission number for each job
        """

        # db connect
        if self.db is None :
            self.connect()

        # defining default
        if jobAttributes is None:
            jobAttributes = {}

        taskList = []
        jobList= jobRange
        # identify jobRange
        if jobRange != 'all' :
            jobSubRanges = jobRange.split(',')
            jobList = []

            for jobSubRange in jobSubRanges :
                if jobSubRange.find(':') == -1 :
                    start = int( jobSubRange )
                    end = int( jobSubRange )
                else :
                    s, e = jobSubRange.split(':')
                    start = int( s )
                    end = int( e )
                jobList.extend( range( start, end+1 ) )

        # loop over tasks
        for subRange in taskRange.split(',') :
            if subRange.find(':') == -1 :
                start = int( subRange )
                end = int( subRange )
            else :
                s, e = subRange.split(':')
                start = int( s )
                end = int( e )

            for taskId in range ( start, end+1 ) :
                
                # create template
                task = Task()
                task['id'] = str(taskId)

                # select jobs
                if jobRange != 'all' :
                    for jobId in jobList:
                        job = Job( jobAttributes )
                        job['jobId'] = str( jobId )
                        task.addJob(job)

                # load task
                task.load(self.db)
                taskList.append( task )

        return taskList
    
    ##########################################################################

        
    def loadJob( self, taskId, jobId ) :
        """
        retrieve job information from db using task and job id
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        jobAttributes = { 'taskId' : taskId, "jobId" : jobId}
        job = Job( jobAttributes )

        # load job from db
        job.load(self.db)

        return job

    ##########################################################################

    def loadJobsByAttr( self, jobAttributes ) :
        """
        retrieve job information from db for job matching attributes
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating jobs
        job = Job( jobAttributes )

        # load job from db        
        jobList = self.db.select(job)

        return jobList



    ##########################################################################

    def getRunningInstance( self, job, runningAttrs = None ) :
        """
        retrieve RunningInstance where existing or create it
        """


        # check whether the running instance is still loaded
        if job.runningJob is not None :
            return

        # set eventual attributes
        if runningAttrs is None :
            runningAttrs = {}

        # load if exixts, create it otherwise
        if job.getRunningInstance(self.db) is None :
            run = RunningJob(runningAttrs)
            job.newRunningInstance( run, self.db )
            run.save(self.db)

    ##########################################################################

    def loadJobsByRunningAttr( self, runningAttrs ) :
        """
        retrieve job information from db for job
        whose running instance match attributes
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating jobs
        run = RunningJob( runningAttrs )

        # load job from db        
        runJobList = self.db.select(run)

        # new job list
        jobList = []

        # recall jobs
        for rJob in  runJobList :
            job = Job(
                { 'jobId' : rJob['jobId'] , 'taskId' : rJob['taskId'] }
                )
            job.runningJob = rJob
            jobList.append( job )

        return jobList

    ##########################################################################

    def loadJobByName( self, jobName ) :
        """
        retrieve job information from db for jobs with name 'name'
        """

        return self.loadJobsByAttr( { 'name' : jobName } )

    ##########################################################################

        
    def loadCreated( self, taskRange="all", jobRange="all") :
        """
        retrieve information from db for jobs created but not submitted using:
        - range of tasks
        - range of jobs inside a task
        
        Takes the highest submission number for each job
        """

        retJobList = []

        # load all jobs from task
        jobList =  self.loadJobsByAttr( {} )

        # evaluate just jobs without running instances or with status W
        for job in jobList :
            job.getRunningInstance( self.db )
            if job.runningJob is None or job.runningJob == [] \
                   or job.runningJob['status'] == 'W' :
                retJobList.append( job )

        return retJobList


    ##########################################################################

    def loadSubmitted( self, taskRange="all", jobRange="all" ) :
        """
        retrieve information from db for jobs submitted using:
        - range of tasks
        - range of jobs inside a task
        
        Takes the highest submission number for each job
        """

        return self.loadJobsByRunningAttr( { 'closed' : None } )

    ##########################################################################

        
    def loadEnded( self, taskRange="all", jobRange="all") :
        """
        retrieve information from db for jobs successfully using:
        - range of tasks
        - range of jobs inside a task
        
        Takes the highest submission number for each job
        """

        return self.loadJobsByRunningAttr( { 'status' : 'SD' } )

    ##########################################################################

        
    def loadFailed( self, taskRange="all", jobRange="all") :
        """
        retrieve information from db for jobs aborted/killed using:
        - range of tasks
        - range of jobs inside a task
        
        Takes the highest submission number for each job
        """

        # load aborted
        jobList = self.loadJobsByRunningAttr( { 'status' : 'SA' } )
        
        # load killed
        jobList.extend( self.loadJobsByRunningAttr( { 'status' : 'SK' } ) )
        
        return jobList

    ##########################################################################

    ## DanieleS.    
    def loadJobDist( self, taskId, value ) :
        """
        retrieve job distinct job attribute 
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        jobAttributes = { 'taskId' : taskId}
        job = Job( jobAttributes )

        # load job from db
        jobList = self.db.distinct(job, value)

        return jobList

    ## DanieleS. NOTE: ToBeRevisited
    def loadJobDistAttr( self, taskId, value_1, value_2, list ) :
        """
        retrieve job distinct job attribute 
        """

        # db connect
        if self.db is None :
            self.connect()

        # creating job
        jobAttributes = { 'taskId' : taskId}
        job = Job( jobAttributes )

        # load job from db
        jobList = self.db.distinctAttr(job, value_1, value_2, list )

        return jobList



    ##########################################################################
  
    def archive( self, task, jobList=None ):
        """
        set a flag/index to closed
        """

        # db connect
        if self.db is None :
            self.connect()

        for job in task.jobs:
            if jobList is None or job['id'] in jobList:
                job.closeRunningInstance( self.db )

        # update
        task.update(self.db)
        self.session.commit()


    ##########################################################################

    def installMySQL( self, schemaLocation ) :
        """
        install MySQL database
        """
        import getpass
        import socket

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
            raise DbError(str(msg))

        # create db
        if create :
            query = 'create database ' + self.dbConfig['dbName']
            try:
                session.execute( query )
                session.commit()
            except Exception, msg:
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
            raise DbError(str(msg))

        # grant user
        query = 'GRANT UPDATE,SELECT,DELETE,INSERT ON ' + \
                self.dbConfig['dbName'] + '.* TO \'' + \
                self.dbConfig['user'] + '\'@\'' + socket.gethostname() \
                + '\' IDENTIFIED BY \''+passwd+'\';'
        try:
            session.execute( query )
            session.commit()
        except Exception, msg:
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


    def deserialize( self, xmlFilePath ) :
        """
        obtain task object from XML object
        """
        from xml.dom import minidom
        doc = minidom.parse( xmlFilePath )

        # parse the task general attributes
        taskNode = doc.getElementsByTagName("TaskAttributes")[0]
        taskInfo =  {}
        for i in range(taskNode.attributes.length):
            key = str(taskNode.attributes.item(i).name)
            val = str(taskNode.attributes.item(i).value)
            if val is None or val == 'None':
                continue
            taskInfo[key] = val

        # run over the task jobs and parse the structure
        jnodes = doc.getElementsByTagName("Job")
        jobs = []
        runningJobsAttribs = {}
        for jobNode in jnodes:
            jobInfo = {}
            for i in range(jobNode.attributes.length):
                key = str(jobNode.attributes.item(i).name)
                val = str(jobNode.attributes.item(i).value)
                if val is None or val == 'None':
                    continue
                jobInfo[key] = val

            jobInfo['inputFiles'] = jobInfo['inputFiles'].split(',')
            jobInfo['outputFiles'] = jobInfo['outputFiles'].split(',')
            jobInfo['dlsDestination'] = jobInfo['dlsDestination'].split(',')
            jobs.append(jobInfo)

            rjAttrs = {}
            rjl = jobNode.getElementsByTagName("RunningJob") 
            if len(rjl) > 0:
                rjNode = rjl[0]
                for i in range(rjNode.attributes.length):
                    key = str(rjNode.attributes.item(i).name)
                    val = str(rjNode.attributes.item(i).value)
                    rjAttrs[key] = val
            runningJobsAttribs[ jobInfo['name'] ] = copy.deepcopy(rjAttrs)
       
        return taskInfo, jobs, runningJobsAttribs

    ##########################################################################

    def serialize( self, task ):
        from xml.dom import minidom

        cfile = minidom.Document()
        root = cfile.createElement("Task")

        node = cfile.createElement("TaskAttributes")
        node.setAttribute('name', str(task['name']) )
        node.setAttribute('startDirectory', str(task['startDirectory']) )
        node.setAttribute('outputDirectory', str(task['outputDirectory']) )
        node.setAttribute('globalSandbox', str(task['globalSandbox']) )
        node.setAttribute('cfgName', str(task['cfgName']) )
        node.setAttribute('serverName', str(task['serverName']) )
        node.setAttribute('jobType', str(task['jobType']) )
        node.setAttribute('scriptName', str(task['scriptName']) )
        root.appendChild(node)

        node = cfile.createElement("TaskJobs")
        for job in task.jobs:
            subNode = cfile.createElement("Job")
            subNode.setAttribute('name', str(job['name']) )
            subNode.setAttribute('executable', str(job['executable']) )
            subNode.setAttribute('arguments', str(job['arguments']) )
            subNode.setAttribute('standardInput', str(job['standardInput']) )
            subNode.setAttribute('standardOutput', str(job['standardOutput']) )
            subNode.setAttribute('standardError', str(job['standardError']) )
            subNode.setAttribute('logFile', str(job['logFile']) )
            subNode.setAttribute('inputFiles', str( ','.join(job['inputFiles'])) )
            subNode.setAttribute('outputFiles', str( ','.join(job['outputFiles'])) )
            subNode.setAttribute('fileBlock', str( job['fileBlock']) )
            subNode.setAttribute('submissionNumber',  str(job['submissionNumber']) )
            #
            dlsD = ','.join(eval(job['dlsDestination']))
            subNode.setAttribute('dlsDestination', dlsD )
            node.appendChild(subNode)

            if job.runningJob is not None:
                subSubNode = cfile.createElement("RunningJob")
                for key in job.runningJob.fields:
                    if not job.runningJob[key] or key in ['id', 'jobId', 'taskId', 'submission'] :
                        continue
                    subSubNode.setAttribute(key, str(job.runningJob[key]) )
                subNode.appendChild(subSubNode)

        root.appendChild(node)
        cfile.appendChild(root)

        # print cfile.toprettyxml().replace('\&quot;','') 
        return cfile.toprettyxml().replace('\&quot;','')




