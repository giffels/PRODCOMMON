#!/usr/bin/env python
"""
__TaskAPITest__

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import  BossLiteAPISched
from ProdCommon.BossLite.Common.BossLiteLogger import BossLiteLogger
from ProdCommon.BossLite.Common.Exceptions import BossLiteError
from ProdCommon.BossLite.Test.DbConfig import dbConfig, dbType
from ProdCommon.BossLite.Test.SchedulerConfig import schedulerConfig
import traceback

import sys, getopt

class TaskAPITests(object):
    """
    TaskAPITests for the Job class
    """

    ##########################################################################
    def __init__(self, dbtype, installDb=False):
        """
        __init__
        """
        
        self.bossSession = None
        self.schedSession = None
        self.task = None
        self.jobRange = 'all'
        self.outdir = None
        self.taskId =  None
        self.taskName =  'test_task'

        # read configuration
        self.database = dbType
        self.dbConfig = dbConfig
        self.schedulerConfig = schedulerConfig

        if dbtype.lower() == 'sqlite' :
            self.bossSession = self._SqLiteSession(installDb)
        elif dbtype.lower() == 'mysql' :
            self.bossSession = self._MySqlSession(installDb)
        else :
            print "bad db choice: '%s', allowed only 'SQLite' or 'MySQL'" % \
                  dbtype
            sys.exit()
        

    ##########################################################################
    def _SqLiteSession(self, installDb):
        """
        __sqLiteSession__
        """

        if self.bossSession is not None :
            return

        # BossLiteApi session
        self.database = "SQLite"
        
        self.bossSession = BossLiteAPI( self.database, self.dbConfig )
        
        # db installed?
        try:
            if installDb :
                self.bossSession.bossLiteDB.installDB(
                    '$PRODCOMMON_ROOT/lib/ProdCommon/BossLite/DbObjects/setupDatabase-sqlite.sql'
                    )
                self.bossSession.bossLiteDB.reset()
        except:
            pass
    
        return self.bossSession


    ##########################################################################
    def _MySqlSession(self, installDb):
        """
        __mySqlSession__
        """

        if self.bossSession is not None :
            return

        self.database = "MySQL"
    
        # BossLiteApi session
        self.bossSession = BossLiteAPI( self.database, self.dbConfig )
    
        # db installed?
        try:
            if installDb :
                self.bossSession.bossLiteDB.installDB(
                    '$PRODCOMMON_ROOT/lib/ProdCommon/BossLite/DbObjects/setupDatabase.sql'
                    )
        except:
            pass
    
        return self.bossSession


    ##########################################################################
    def schedulerSession(self):
        """
        __schedulerSession__
        """

        if self.schedSession is not None:
            return self.schedSession

        self.schedSession = BossLiteAPISched( self.bossSession, \
                                                  self.schedulerConfig, \
                                                  self.task )
        return self.schedSession


    ##########################################################################
    def testTask( self ) :
        """
        __testTask__
        """
        
        try:
            if self.taskId is not None :
                self.load( self.taskId, self.jobRange)
            else :
                self.task = self.bossSession.loadTaskByName( self.taskName,
                                                             deep=False)
                self.load( self.task, self.jobRange)
            print "Task loaded..."
        except BossLiteError, e:
            print "Task not found... declaring"

 
            taskParams = {'name' : self.taskName,
                          'globalSandbox' : '/etc/redhat-release' }
            
            self.task = Task( taskParams )
            print self.task
            
            parameters = {'executable' : '/bin/echo',
                          'arguments' : 'ciao',
                          'standardError' : 'err.txt',
                          'standardOutput' : 'out.txt',
                          'outputFiles' : ['out.txt']}
    #                      'outputFiles' : ['err.txt', 'out.txt', '.BrokerInfo']}
            jobs = []
            for jobId in range(1, 51):
                parameters['name'] = 'job' + str(jobId)
                job = Job(parameters)
                self.bossSession.getNewRunningInstance(job)
                jobs.append(job)
    
            parameters['arguments'] = 'ciao2'
            for jobId in range(51, 101):
                parameters['name'] = 'job' + str(jobId)
                job = Job(parameters)
                self.bossSession.getNewRunningInstance(job)
                jobs.append(job)

            self.task.addJobs(jobs)
            self.bossSession.saveTask( self.task )
    
            for job in self.task.jobs :
                print job['jobId'], job['taskId'], job['submissionNumber'],
                if job.runningJob is not None :
                    print job.runningJob['jobId'], \
                          job.runningJob['taskId'], \
                          job.runningJob['submission']
    
        return self.task


    ##########################################################################
    def printTask( self ) :
        """
        __printTask__
        """
        
        for job in self.task.jobs :
            print job['jobId'], job['taskId'], job['submissionNumber'],
            if job.runningJob is None :
                print ''
            else :
                print job.runningJob['submission'], \
                 job.runningJob['schedulerId'], \
                 job.runningJob['statusScheduler'], \
                 job.runningJob['service'], \
                 job.runningJob['lbTimestamp']


    ##########################################################################
    def submit(self) :
        """
        __submitTask__
        """

        if self.task is None :
            self.task = self.testTask()

        schedSession = self.schedulerSession()
        self.task = schedSession.submit( self.task, self.jobRange )
        self.printTask()


    ##########################################################################
    def resubmit(self) :
        """
        __reSubmitTask__
        """

        if self.task is None :
            self.task = self.testTask()
        
        schedSession = self.schedulerSession()
        self.task = schedSession.resubmit( self.task, self.jobRange )
        self.printTask()


    ##########################################################################

    def resubmitLong(self) :
        """
        __reSubmitTaskBis__
        """
        
        if self.task is None :
            self.task = self.testTask()
        
        schedSession = self.schedulerSession()
    
        self.task = self.bossSession.load(self.taskId, self.jobRange)[0]
        for job in self.task.jobs:
            self.bossSession.getNewRunningInstance( job )
            
        self.bossSession.updateDB(self.task)

        #task = self.bossSession.load(self.taskId, self.jobRange)[0]
        self.task = schedSession.submit( self.task )
        self.printTask()


    ##########################################################################
    def jobDescription( self) :
        """
        __jobDescription__
        """
        
        if self.task is None :
            self.task = self.testTask()


        schedSession = self.schedulerSession()
        print schedSession.jobDescription( self.task, self.jobRange )


    ##########################################################################
    def query(self) :
        """
        __query__
        """

        if self.task is None :
            self.task = self.testTask()
    
        schedSession = self.schedulerSession()

        self.task = schedSession.query( self.task, self.jobRange )
        self.printTask()
        del schedSession


    ##########################################################################
    def kill(self) :
        """
        __killTask__
        """
        
        if self.task is None :
            self.task = self.testTask()
    
        schedSession = self.schedulerSession()
        self.task = schedSession.kill( self.task, self.jobRange )
        self.printTask()


    ##########################################################################
    def getOutput(self) :
        """
        __getOutTask__
        """
        
        if self.task is None :
            self.task = self.testTask()
    
        schedSession = self.schedulerSession()
        self.task = schedSession.getOutput( self.task, self.jobRange, \
                                            self.outdir )
        self.printTask()


    ##########################################################################
    def load(self, taskId, jobRange) :
        """
        __loadTask__
        """

        self.taskId = taskId
        self.jobRange = jobRange
        self.task = self.bossSession.load(taskId, jobRange)[0]
        self.taskName = self.task['name']
        return self.task

    ##########################################################################
    def removeTask(self) :
        """
        __loadTask__
        """

        self.bossSession.removeTask(self.task)
        self.task = None

    ##########################################################################
    def _SQL(self) :
        """
        ____SQL__
        """

        results = self.bossSession.bossLiteDB.select(self.sqlQuery)
        if results is None:
            return
        for row in results:
            for field in row :
                print field,
            print ''


    ##########################################################################


def usage(lMethods) :
    """
    __usage__
    """
    
    print '  to build a test task use the --test option'
    print '  otherwise specify a valid taskId (--taskId) or taskName (--taskName)'
    print '        and eventually a taskRange (--jobRange)'
    print '  then select an action among  :'
    print lMethods

########## MAIN ##########
def main():
    """
    __main__
    """

    test = False
    actions = []
    jobRange = 'all'
    taskId =  None
    taskName =  None
    dbtype = 'sqlite'
    outdir = None
    installDB = False
    sqlQuery = None

    lOptions = [ func for func in TaskAPITests.__dict__.keys()
                 if func[0] != '_' ]
    lMethods = [ '--' + func for func in lOptions ]
    lOptions.extend( ["help", 'test', 'installDB', \
                      'sql=', 'taskId=', 'taskName=', 'jobRange=', 'outdir='] )

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:v", lOptions)
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage(lMethods)
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(lMethods)
            sys.exit()
        elif o in ("--outdir"):
            outdir = a
        elif o in ("--taskId"):
            taskId = a
        elif o in ("--taskName"):
            taskName = a
        elif o in ("--jobRange"):
            jobRange = a
        elif o in ("--installDB"):
            installDB = True
        elif o in ("--sql"):
            sqlQuery = a
        elif o in ("--test"):
            test = True
        elif o in lMethods:
            actions.append( o[2:] )
        else :
            assert False, "unhandled option"

    if sqlQuery is not None and actions == []:
        bossSession = TaskAPITests(dbtype, installDB)
        bossSession.sqlQuery = sqlQuery
        bossSession._SQL()
        sys.exit()

    if taskId is None and taskName is None and not test:
        usage(lMethods)
        sys.exit()
    
    # initialize test session and load or create task
    bossSession = TaskAPITests(dbtype, installDB)
    bossSession.taskId = taskId
    if taskName is not None :
        bossSession.taskName = taskName
    bossSession.sqlQuery = sqlQuery
    bossSession.jobRange = jobRange
    bossSession.outdir = outdir
    bossSession.testTask()

    for a in actions :
        print 'action : ', a, '  for jobs', bossSession.jobRange, 
        print '   of task', bossSession.task['id']
        try:
            TaskAPITests.__dict__[a](bossSession)
            print BossLiteLogger( bossSession.task )
        except Exception, e:
            print BossLiteLogger( bossSession.task, e )


########## END MAIN ##########

if __name__ == "__main__":
    main()

