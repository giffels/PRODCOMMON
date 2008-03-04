#!/usr/bin/env python
"""
_Task_

"""

__version__ = "$Id: Task.py,v 1.2 2008/02/21 16:27:34 gcodispo Exp $"
__revision__ = "$Revision: 1.2 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os.path

from ProdCommon.BossLite.DbObjects.DbObject import DbObject
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError, DbError

class Task(DbObject):
    """
    Task object
    """

    # fields on the object and their names on database
    fields = { 'id' : 'id',
               'name' : 'name',
               'startDirectory' : 'start_dir',
               'outputDirectory' : 'output_dir',
               'globalSandbox' : 'global_sanbox',
               'cfgName' : 'cfg_name',
               'serverName' : 'server_name',
               'jobType' : 'job_type',
               'scriptName' : 'script_name',
               'user_proxy' : 'user_proxy'
             }

    # mapping between field names and database fields
    mapping = fields

    # default values for fields
    defaults = { 'id' : None,
                 'name' : None,
                 'startDirectory' : None,
                 'outputDirectory' : None,
                 'globalSandbox' : None,
                 'cfgName' : None,
                 'serverName' : None,
                 'jobType' : None,
                 'scriptName' : None,
                 'user_proxy' : None
              }

    # database properties
    tableName = "bl_task"
    tableIndex = ["id"]

    # exception class
    exception = TaskError

    ##########################################################################

    def __init__(self, parameters = {}):
        """
        initialize a Task instance
        """

        # call super class init method
        super(Task, self).__init__(parameters)

        # initialize job set struture
        self.jobs = []
        self.jobIndex = 0

    ##########################################################################

    def addJob(self, job):
        """
        insert a job into the task
        """

        # assign id to the job
        self.jobIndex += 1
        job['jobId'] = self.jobIndex

        # assign task id if possible
        if self.data['id'] is not None:
            job['taskId'] = self.data['id']

        # insert job
        self.jobs.append(job)

    ##########################################################################

    def addJobs(self, listOfJobs):
        """
        insert jobs into the task
        """

        for job in listOfJobs:
            self.addJob(job)

    ##########################################################################

    def getJobs(self):
        """
        return the list of jobs in task
        """

        return self.jobs

    ##########################################################################

    def save(self, db):
        """
        save complete task object in database
        """

        # verify if the task is new 
        if self.valid(['id']):
            raise TaskError("The following task instance cannot be saved, " + \
                      "since it specifies an ID. Consider using " + \
                      "update instead of save: %s" % self)

        # verify that the task has a name
        if not self.valid(['name']):
            raise TaskError("The following task instance cannot be saved, " + \
                      "since it is not completely specified: %s" % \
                      self)

        # insert complete task
        try:

            # insert task
            status = db.insert(self)
            if status != 1:
                raise TaskError("Cannot insert task %s" % self)

            # get task id
            listOfTasks = db.select(self)
            if len(listOfTasks) != 1:
                raise TaskError("Cannot insert task %s" % self)

            # store task id in task and in all jobs
            self.data['id'] = listOfTasks[0]['id']
            for job in self.jobs:
                job['taskId'] = self.data['id']

            # store all jobs
            for job in self.jobs:
                status += job.save(db)

        # database error
        except DbError, msg:
            raise TaskError(str(msg))

        # job error
        except JobError, msg:
            raise TaskError(str(msg))

        # update status
        self.existsInDataBase = True

        return status


    ##########################################################################

    def remove(self, db):
        """
        remove task object from database (with all jobs)
        """

        # verify data is complete
        if not self.valid(['id']):
            raise TaskError("The following task instance cannot be removed" + \
                      " since it is not completely specified: %s" % self)

        # remove from database
        try:
            status = db.delete(self)
            if status < 1:
                raise TaskError("Cannot remove task %s" % str(self))

        # database error
        except DbError, msg:
            raise TaskError(str(msg))

        # update status
        self.existsInDataBase = False

        # return number of entries removed
        return status

   ##########################################################################

    def update(self, db, deep = True):
        """
        update task object from database (with all jobs)
        """

        # verify if the object exists in database
        if not self.existsInDataBase:

            # no, use save instead of update
            return self.save(db)

        # verify data is complete
        if not self.valid(['id']):
            raise TaskError("The following task instance cannot be updated," + \
                     " since it is not completely specified: %s" % self)

        # update task object in database
        try:
            status = db.update(self)

            # update all jobs
            if deep:
                for job in self.jobs:
                    status += job.update(db)

        # database error
        except DbError, msg:
            raise TaskError(str(msg))

        # job error
        except JobError, msg:
            raise TaskError(str(msg))

        # return number of entries updated
        return status

   ##########################################################################

    def load(self, db, deep = True):
        """
        load information from database
        """

        # verify data is complete
        if not self.valid(['id']) and not self.valid(['name']):
            raise TaskError("The following task instance cannot be loaded" + \
                     " since it is not completely specified: %s" % self)

        # get information from database based on template object
        try:
            objects = db.select(self)

        # database error
        except DbError, msg:
            raise TaskError(str(msg))

        # since required data is a key, it should be a single object list
        if len(objects) == 0:
            raise TaskError("No task instances corresponds to the," + \
                     " template specified: %s" % self)

        if len(objects) > 1:
            raise TaskError("Multiple task instances corresponds to the" + \
                     " template specified: %s" % self)

        # copy fields
        for key in self.fields:
            self.data[key] = objects[0][key]

        # get job objects if requested
        if deep:

            # create basic template if not provided
            if self.jobs == []:
                template = Job({'taskId' : self.data['id']})

            # select single job as template checking proper specification
            else:
                if len(self.jobs) != 1:
                    raise TaskError("Multiple job instances specified as" + \
                     " templates in load task operation for task: %s" % self)

                # update template for matching
                template = self.jobs[0]
                template['taskId'] = self['id']
                template['jobId'] = None

            # get jobs
            self.jobs = db.select(template)

            # get associated running jobs (if any)
            for job in self.jobs:
                job.getRunningInstance(db)

        # update status
        self.existsInDataBase = True

        # update job status
        for job in self.jobs:
            job.existsInDataBase = True

        # update private data
        self.updateInternalData()

   ##########################################################################

    def updateInternalData(self):
        """
        update private information on it and on its jobs
        """

        # get output directory
        if self.data['outputDirectory'] is not None:
            outputDirectory = self.data['outputDirectory']
        else:
            outputDirectory = ""

        # update job status and private information
        for job in self.jobs:

            # comput full path for output files
            job['fullPathOutputFiles'] = [os.path.join(outputDirectory, file)
                                          for file in job['outputFiles']]

        # get input directory
        if self.data['globalSandbox'] is not None:
            inputDirectory = self.data['globalSandbox']
        else:
            inputDirectory = ""

        # update job status and private information
        for job in self.jobs:

            # comput full path for output files
            job['fullPathInputFiles'] = [os.path.join(inputDirectory, file)
                                         for file in job['inputFiles']]

