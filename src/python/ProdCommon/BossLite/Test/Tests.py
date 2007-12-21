#!/usr/bin/env python
"""
Unittest for tasks and jobs API

INFO:

+ Set specific parameters into configuration()
+ Timing information on file Tests.info
 
TESTS:

testCreation1:   creates a task with 2 jobs
testCreation2:   creates a task with N jobs
testCreation3:   tests with running jobs
testLoading1:    loads and verifies both tasks
testLoading2:    partial loading of a task with N jobs
testLoading3:    load of a single task
testLoading4:    verify update of private (non database) fields
testUpdate1:     update (load + update + save) task information

"""

__version__ = "$Id$"
__revision__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import sys
import unittest
import logging
from time import time
import os

# Database imports
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.Database.SafePool import SafePool
from BossLite.DbObjects.TrackingDB import TrackingDB
from ProdCommon.Database.MysqlInstance import MysqlInstance
from ProdCommon.Database.SqliteInstance import SqliteInstance

# Database configuration
from ProdAgentDB.Config import defaultConfig as dbConfig

# Task and job objects
from BossLite.DbObjects.Job import Job
from BossLite.DbObjects.Task import Task
from BossLite.DbObjects.RunningJob import RunningJob
from BossLite.Common.Exceptions import *

##############################################################################

class jobUnitTests(unittest.TestCase):
    """
    TestCase for the Job class
    """

    ##########################################################################

    def configure(self):
        """
        set parameters (possible values in comment)
        """

        self.database = "MySQL"       # "MySQL" or "SQLite"
        self.N = 20

    ##########################################################################

    def __init__(self, argv):
        """
        initialize test instance
        """

        # initialize super class
        super(jobUnitTests, self).__init__(argv)

        # read configuration
        self.configure()

        # MySQL: get DB configuration from config file
        if self.database == "MySQL":
            from ProdAgentCore.Configuration import loadProdAgentConfiguration
            cfg = loadProdAgentConfiguration()
            dbConfig = cfg.getConfig("ProdAgentDB")
            dbConfig['dbName'] += '_BOSS'

            # create DB instance
            self.dbInstance = MysqlInstance(dbConfig)

        else:
            dbConfig = {}
            dbConfig['dbName'] = 'ProdAgentDB_BOSS'

            # create DB instance
            self.dbInstance = SqliteInstance(dbConfig)

        # remove all information from database
        session = SafeSession(dbInstance = self.dbInstance)

        try:
            session.execute("delete from bl_runningjob")
            session.execute("delete from bl_job")
            session.execute("delete from bl_task")
            session.commit()
        except DbError, msg:
            print "Cannot erase database contents: " + str(msg)
            sys.exit(1)

        session.close()

        # set logging facility
        logging.basicConfig(filename = 'Tests.info', filemode = 'w', \
                            level=logging.INFO)

    ##########################################################################

    def setUp(self):
        """
        perform actions before any single test
        """

        pass

    ##########################################################################

    def testCreation1(self):
        """
        creates a single task with 2 jobs
        """

        # log information
        print "Creation tests"
        logging.info("Test Creation 1: creating a task with two jobs")
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create a task
        task = Task()
        task['name'] = 'task1'
        task['startDirectory'] = '/tmp/startdir'
        task['outputDirectory'] = '/tmp/output'
        task['globalSandbox'] = '/tmp/inputdata'

        # create first job
        job1 = Job()
        job1['name'] = 'job1'
        job1['executable'] = 'test.sh'
        job1['inputFiles'] = ['a.txt', 'b.txt']
        job1['outputFiles'] = ['c.txt']

        # create second job
        job2 = Job()
        job2['name'] = 'job2'
        job2['executable'] = 'production.sh'
        job2['arguments'] = "-o c.txt"

        # add jobs to task
        task.addJobs([job1, job2])

        # save task in single transaction
        try:
            rows = task.save(db)
            session.commit()
        except TaskError, msg:
            self.fail("Error: " + str(msg))

        # close session
        session.close()

        # three records must have been created
        self.assertEqual(rows, 3)

        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testCreation2(self):
        """
        creates a single task with N jobs
        """

        # log information
        logging.info("Test Creation 2: creating a task with %s jobs" % self.N)
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create a task
        task = Task()
        task['name'] = 'task2'
        task['startDirectory'] = '/tmp/startdir'
        task['outputDirectory'] = '/tmp/outdir'
        task['globalSandbox'] = '/tmp/data_area'

        # create template for jobs
        template = { 'inputFiles' : ['a.txt', 'b.txt'],
                     'executable' : 'production.sh',
                     'arguments' : "-o c.txt",
                     'outputFiles' : ['c.txt'],
                     'logFile' : 'output.log' }

        # create list of N jobs
        jobs = []
        for index in range(1, self.N + 1):

            # set specific parameters
            template['name'] = 'job' + str(index)
            if index <= int(self.N / 2):
                template['logFile'] = 'output.log'
            else:
                template['logFile'] = 'useful.log'
            template['outputFiles'] = ['a.txt' , 'b.txt']

            # add job to list
            jobs.append(Job(template))

        # add jobs to task
        task.addJobs(jobs)

        # save task (and jobs) in single transaction
        try:
            rows = task.save(db)
            session.commit()
        except TaskError, msg:
            self.fail("Error: " + str(msg))

        # close session
        session.close()

        # N + 1 records must have been created
        self.assertEqual(rows, self.N + 1)

        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testLoading1(self):
        """
        deep load of a task with 2 jobs and a task with N jobs
        """

        # log information
        print "\nLoading tests"
        logging.info("Test Loading 1: deep load of tasks with 2 and %s jobs" \
                     % self.N)
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template
        task = Task()
        task['name'] = 'task1'

        # load complete task
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # check for task information
        self.assertEqual(task['startDirectory'], "/tmp/startdir")
        
        # check for two jobs
        self.assertEqual(len(task.jobs), 2)

        # check job names
        jobNames = set(['job'+str(index) for index in range(1, 3)])
        for job in task.jobs:
            if job['name'] in jobNames:
                jobNames.remove(job['name'])
            else:
                self.fail("Wrong job in task object")
        self.assertEqual(len(jobNames), 0)

        # get seconds task
        task = Task()
        task['name'] = 'task2'

        # load complete task
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # close session
        session.close()

        # check for task information
        self.assertEqual(task['outputDirectory'], "/tmp/outdir")

        # check for N jobs
        self.assertEqual(len(task.jobs), self.N)

        # check job names
        jobNames = set(['job'+str(index) for index in range(1, self.N + 1)])
        for job in task.jobs:
            if job['name'] in jobNames:
                jobNames.remove(job['name'])
            else:
                self.fail("Wrong job in task object")
        self.assertEqual(len(jobNames), 0)

        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testLoading2(self):
        """
        partial loading of a task with N jobs
        """

        # log information
        logging.info("Test Loading 2: partial load of a task %s jobs" % self.N)
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for task
        task = Task()
        task['name'] = 'task2'

        # create template for jobs with particular logFile
        job = Job()
        job['logFile'] = 'output.log'

        # add to task template
        task.addJob(job)

        # load partial task (jobs with logFile 'output.log')
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # close session
        session.close()

        # check for N/2 jobs
        self.assertEqual(len(task.jobs), int(self.N / 2))

        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testLoading3(self):
        """
        loading of a single job in the N jobs task
        """

        # log information
        logging.info("Test Loading 3: load of a single job in a %s jobs task" \
                     % self.N)
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for task
        task = Task()
        task['name'] = 'task2'

        # create template for jobs with particular logFile
        job = Job()
        job['name'] = 'job1'

        # add to task template
        task.addJob(job)

        # load partial task (jobs with logFile 'output.log')
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # close session
        session.close()

        # check for 1 jobs
        self.assertEqual(len(task.jobs), 1)

        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testLoading4(self):
        """
        checking update of full output path for output files and input files
        """
        # log information
        logging.info("Test Loading 4: testing full path operations") 
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for task
        task = Task()
        task['name'] = 'task2'

       # load task
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # close session
        session.close()

        # check for all jobs that output full path is correctly computed
        for job in task.jobs:
            for file, full in zip(job['outputFiles'],
                                  job['fullPathOutputFiles']):
                self.assertEqual(os.path.join(task['outputDirectory'], file),
                                 full)

        # check for all jobs that input full path is correctly computed
        for job in task.jobs:
            for file, full in zip(job['inputFiles'],
                                  job['fullPathInputFiles']):
                self.assertEqual(os.path.join(task['globalSandbox'], file),
                                 full)
        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testUpdate1(self):
        """
        updating task object
        """
        # log information
        logging.info("Test Updating 1: updating task object")
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for task
        task = Task()
        task['name'] = 'task2'

        # load task
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # update all jobs
        for job in task.jobs:
            job['standardError'] = 'output.err'

        # update information about task in database
        try:
            task.update(db)
            session.commit
        except TaskError, msg:
            self.fail("Cannot update task: " + str(msg))

        # close session
        session.close()

        #####################################
        # verify operation
        #####################################

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for task
        task = Task()
        task['name'] = 'task2'

       # load task
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # update all jobs
        for job in task.jobs:
            if job['standardError'] != 'output.err':
                self.fail("Wrong update for scheduler attributes")

        logging.info("    took " + str(time() - start) + " seconds")

    ##########################################################################

    def testCreation3(self):
        """
        creating a running instance
        """

        # log information
        logging.info("Test Updating 1: updating task object")
        start = time()

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for job
        task = Task({'name' : 'task1'})
        job = Job({'name' : 'job1'})
        task.addJob(job)

        # load information
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # creating a running instance
        runJob = RunningJob()
        runJob['schedulerId'] = \
                 "https://lb102.cern.ch:9000/NcEVz8_dJ2tI0YP6sBmHNQ"
        runJob['executionHost'] = "ce111.cern.ch"
        runJob['status'] = 'R'
        task.jobs[0].newRunningInstance(runJob, db)
       
        # update information about task in database
        try:
            task.update(db)
            session.commit()
        except TaskError, msg:
            self.fail("Cannot update task: " + str(msg))

        # creating a new running instance (closing automatically previous)
        runJob = RunningJob()
        runJob['schedulerId'] = \
                 "https://lb102.cern.ch:9000/NcEVz8_dJ2tI0YP6sBmHNQ"
        runJob['executionHost'] = "ce100.cnaf.infn.it"
        runJob['status'] = 'R'
        task.jobs[0].newRunningInstance(runJob, db)

        # update information about task in database
        try:
            task.update(db)
            session.commit
        except TaskError, msg:
            self.fail("Cannot update task: " + str(msg))

        # close session
        session.close()

        #####################################
        # verify operation
        #####################################

        # create a session and db access
        session = SafeSession(dbInstance = self.dbInstance)
        db = TrackingDB(session)

        # create template for job
        task = Task({'name' : 'task1'})
        job = Job({'name' : 'job1'})
        task.addJob(job)

        # load task
        try:
            task.load(db)
        except TaskError, msg:
            self.fail("Cannot load task: " + str(msg))

        # get runnig job information
        runJob = task.jobs[0].runningJob
        if runJob['executionHost'] != 'ce100.cnaf.infn.it':
                self.fail("Wrong creation of running job")

        logging.info("    took " + str(time() - start) + " seconds")


##############################################################################


if __name__ == '__main__':
    unittest.main()

