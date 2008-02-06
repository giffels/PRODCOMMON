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
from BossLite.DbObjects.Job import Job
from BossLite.DbObjects.Task import Task
from BossLite.DbObjects.RunningJob import RunningJob
from BossLite.Common.Exceptions import *

# Scheduler interaction
from BossLite.Scheduler import Scheduler

from os.path import expandvars

##########################################################################

class BossLiteAPI(object):
    """
    High level API class for DBObjcets and Scheduler interaction.
    It allows load/operate/update jobs and taks using just id ranges
    
    """

    def __init__(self, database, dbConfig, schedulerConfig):
        """
        initialize the API instance
        - database can be both MySQl or SQLite
        - dbConfig is in the form ....
        - schedulerConfig is a dictionary with the format
           {'name' : 'SchedulerGLiteAPI',
            'user_proxy' : '/proxy/path',
            'service' : 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server',
            'config' : '/etc/glite_wms.conf' }

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

        # update scheduler config
        self.schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
        self.schedConfig.update( schedulerConfig )

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

        # scheduler
        self.scheduler = Scheduler.Scheduler(
            schedulerConfig['name'], self.schedConfig
            )

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
        
    def declare( self, xml ) :
        """
        register job related informations in the db
        """
        
        task = self.deserialize( xml )
        self.saveTask( task )

        return task


    ##########################################################################

    def saveTask( self, task ):
        """
        register task related informations in the db
        """

        # db connect
        if self.db is None :
            self.connect()

        # save task
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
                        job['id'] = str( jobId )
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
        jobAttributes = { 'taskid' : taskId, jobId : 'id'}
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
                { 'id' : rJob['jobId'] , 'taskId' : rJob['taskId'] }
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


    def submit( self, task, jobRange='all', requirements='', jobAttributes=None ):
        """
        works for Task objects
        
        just create running instances and submit to the scheduler
        in case of first submission
        
        archive existing submission an create a new entry for the next
        submission (i.e. duplicate the entry with an incremented
        submission number)
        """

        # db connect
        if self.db is None :
            self.connect()

        # load and close eventual running instances
        #for job in task.jobs:
        #    if jobRange != 'all' and job['id'] in jobRange:
        #        job.closeRunningInstance( self.db )

        # update changes
        #task.update(self.db)
        #self.session.commit()

        # create or recreate running instances
        for job in task.jobs:
            if jobRange == 'all' or job['id'] in jobRange:
                self.getRunningInstance(
                    job, { 'schedulerAttributes' : jobAttributes }
                    )

        # scheduler submit
        self.scheduler.submit( task, requirements )

        # update
        task.update(self.db)
        self.session.commit()


    ##########################################################################


    def resubmit( self, taskId, jobRange='all', requirements='', jobAttributes=None ):
        """
        unlike previous method, works using taskId
        and load itself the Task object
        
        archive existing submission an create a new entry for the next
        submission (i.e. duplicate the entry with an incremented
        submission number)
        """

        # db connect
        if self.db is None :
            self.connect()

        # load and close running instances
        task = self.load( taskId, jobRange )[0]

        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                job.closeRunningInstance( self.db )

        # update changes
        task.update(self.db)
        self.session.commit()

        # get new running instance
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.getRunningInstance(
                    job, { 'schedulerAttributes' : jobAttributes }
                    )

        # scheduler submit
        self.scheduler.submit( task, requirements )

        # update
        task.update(self.db)
        self.session.commit()

    ##########################################################################

    def query( self, taskId, jobRange='all', queryType='node' ):
        """
        query status and eventually other scheduler related information
        """

        # db connect
        if self.db is None :
            self.connect()

        task = self.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.getRunningInstance( job )

        # scheduler query
        self.scheduler.query( task, queryType )

        # update
        task.update(self.db)
        self.session.commit()

    ##########################################################################


    def getOutput( self, taskId, jobRange='all', outdir='' ):
        """
        retrieve output or just put it in the destination directory
        """

        # db connect
        if self.db is None :
            self.connect()

        task = self.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.getRunningInstance( job )

        # scheduler query
        self.scheduler.query( task, outdir )

        # update
        task.update(self.db)
        self.session.commit()

    ##########################################################################
    
    def kill( self, taskId, jobRange='all' ):
        """
        kill the job instance
        """

        task = self.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.getRunningInstance( job )

        # scheduler query
        self.scheduler.kill( task )

        # update
        task.update(self.db)
        self.session.commit()

    ##########################################################################


    def matchResources( self, taskId, jobRange='all', requirements='', jobAttributes=None ) :
        """
        perform a resources discovery
        """

        # db connect
        if self.db is None :
            self.connect()

        # create a session and db access
        task = self.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.getRunningInstance(
                    job, { 'schedulerAttributes' : jobAttributes }
                    )

        # scheduler query
        return self.scheduler.matchResources( task, requirements )


    ##########################################################################
        
    def jobDescription ( self, taskId, jobRange='all', requirements='', jobAttributes=None ):
        """
        query status and eventually other scheduler related information
        """

        # db connect
        if self.db is None :
            self.connect()

        task = self.load( taskId, jobRange )[0]

        for job in task.jobs:
            self.getRunningInstance(
                job, { 'schedulerAttributes' : jobAttributes }
                )

        return self.scheduler.jobDescription ( task, requirements )

    ##########################################################################

    def purgeService( self, taskId, jobRange='all') :
        """
        purge the service used by the scheduler from job files
        not available for every scheduler
        """
        
        # db connect
        if self.db is None :
            self.connect()

        # retrieve and purge task
        task = self.load( taskId, jobRange )[0]
        self.scheduler.purgeService( task )

        # update
        task.update(self.db)
        self.session.commit()

    ##########################################################################

    def postMortem ( self, taskId, jobId, outfile ) :
        """
        execute any post mortem command such as logging-info
        """

        # db connect
        if self.db is None :
            self.connect()

        # create a session and db access
        task = self.loadTask( taskId, {'id' : jobId} )

        # retrieve running instances
        for job in task.jobs:
            self.getRunningInstance( job )

        # scheduler query
        self.scheduler.postMortem( task, outfile )

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

    def deserialize( self, xmlString ) :
        """
        obtain task object from XML object
        """
        
        from xml.dom import minidom

        # get the doc root
        doc = minidom.parseString(xmlString)
        root = doc.documentElement

        # parse the task general attributes
        node = root.getElementsByTagName("TaskAttributes")
        taskInfo =  \
                 { 'name' : node.getAttribute('name'),
                   'startDirectory' : node.getAttribute('startDirectory'),
                   'outputDirectory' : node.getAttribute('outputDirectory'),
                   'globalSandbox' : list( node.getAttribute('globalSandbox') )
                   }

        # run over the task jobs and parse the structure
        node = root.getElementsByTagName("TaskJobs")
        jobs = []

        for jobNode in node.childNodes:
            jobInfo = \
                    { 'jobId' : jobNode.getAttribute('jobId'),
                      'name' : jobNode.getAttribute('name'),
                      'executable' : jobNode.getAttribute('executable'),
                      'arguments' : jobNode.getAttribute('arguments'),
                      'taskId' : jobNode.getAttribute('taskId'),
                      'standardInput' : jobNode.getAttribute('standardInput'),
                      'standardOutput' : jobNode.getAttribute('standardOutput'),
                      'standardError' : jobNode.getAttribute('standardError'),
                      'logFile' : jobNode.getAttribute('logFile'),
                      'inputFiles' : list(jobNode.getAttribute('inputFiles')),
                      'outputFiles' : list(jobNode.getAttribute('outputFiles')),
                      'submissionNumber' : int(jobNode.getAttribute('submissionNumber'))
                      }
            job = Job( jobInfo )

            if jobNode.childNodes.lenght > 0:
                # db connect
                if self.db is None :
                    self.connect()

                # the RunningJob Section exists
                run = {}
                rjNode = jobNode.childNodes[0]
    
                for i in range(rjNode.attributes.length):
                    run[str(rjNode.attributes.item(i).name)] = str(rjNode.attributes.item(i).value)
                    
                job.newRunningInstance(run, self.db )

                self.close()
            
            jobs.append(job)

        # deserializartion completed:
        # // create the actual task ad add its own (running)jobs
        task = Task(taskInfo)    
        task.addJobs(jobs)
        return task
    

    ##########################################################################


    def serialize( self, task ) :
        """
        obtain XML object from task object

        <Task>!
            <TaskAttributes ... >!
            <TaskJobs>
                <Job ...>
                <RunningJob ...>!
                </Job >+
            </TaskJobs>!
        </Task>
        """
        
        from xml.dom import minidom

        cfile = minidom.Document()
        
        root = cfile.createElement("Task")
        node = root.createElement("TaskAttributes")
        node.setAttribute('id', task['id'])
        node.setAttribute('name', task['name'])
        node.setAttribute('startDirectory', task['startDirectory'] )
        node.setAttribute('outputDirectory', task['outputDirectory'] )
        node.setAttribute('globalSandbox',  ','.join( task['globalSandbox'] ) )
        root.appendChild(node)

        node = root.createElement("TaskJobs")        
        for job in task.jobs:
            subNode = node.createElement("Job")
            subNode.setAttribute('jobId', job['jobId'])
            subNode.setAttribute('name', job['name'])
            subNode.setAttribute('executable', job['executable'])
            subNode.setAttribute('arguments', job['arguments'])
            subNode.setAttribute('taskId', job['taskId'])
            subNode.setAttribute('standardInput', job['standardInput'])
            subNode.setAttribute('standardOutput', job['standardOutput']) 
            subNode.setAttribute('standardError', job['standardError'])
            subNode.setAttribute('logFile', job['logFile'])
            subNode.setAttribute('inputFiles', ','.join( job['inputFiles']))
            subNode.setAttribute('outputFiles', ','.join( job['outputFiles']))
            subNode.setAttribute('submissionNumber',  job['submissionNumber'])
            
            if job.runningJob is not None:
                subSubNode = subNode.createElement("RunningJob")
                for key, val in job.runningJob :
                    subSubNode.setAttribute(key, val)
                subNode.appendChild(subSubNode)

            node.appendChild(subNode)
            
        root.appendChild(node)
        cfile.appendChild(root)

        return cfile.toxml()






