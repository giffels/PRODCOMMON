#!/usr/bin/env python
"""
_Job_

"""

__version__ = "$Id$"
__revision__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

from copy import deepcopy

from ProdCommon.BossLite.Common.Exceptions import JobError, DbError
from ProdCommon.BossLite.DbObjects.DbObject import DbObject
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class Job(DbObject):
    """
    Job object
    """

    # fields on the object and their names on database
    fields = { 'id' : 'id',
               'jobId' : 'job_id',
               'name' : 'name',
               'executable' : 'executable',
               'arguments' : 'arguments',
               'taskId' : 'task_id',
               'standardInput' : 'stdin',
               'standardOutput' : 'stdout',
               'standardError' : 'stderr',
               'logFile' : 'log_file',
               'inputFiles' : 'input_files',
               'outputFiles' : 'output_files',
               'submissionNumber' : 'submission_number',
               'schedulerAttributes' : 'sched_attr'
              }

    # mapping between field names and database fields
    mapping = fields

    # private non mapped fields
    private = { 'fullPathOutputFiles' : [],
                'fullPathInputFiles' : [] }

    # default values for fields
    defaults = { 'id' : None,
                 'jobId' : None,
                 'name' : None,
                 'executable' : None,
                 'arguments' : "",
                 'taskId' : None,
                 'standardInput' : "",
                 'standardOutput' : "",
                 'standardError' : "",
                 'logFile' : None,
                 'inputFiles' : [],
                 'outputFiles' : [],
                 'submissionNumber' : 0,
                 'schedulerAttributes' : None
              }

    # database properties
    tableName = "bl_job"
    tableIndex = ["jobId", "taskId"]

    # exception class
    exception = JobError

    ##########################################################################

    def __init__(self, parameters = {}):
        """
        initialize a Job instance
        """

        # call super class init method
        super(Job, self).__init__(parameters)

        self.privateData = deepcopy(self.__class__.private)

        # initialize running job
        self.runningJob = None

    ##########################################################################

    def save(self, db):
        """
        save complete job object in database
        """

        # verify data is complete
        if not self.valid(['jobId', 'taskId', 'name']):
            raise JobError("The following job instance cannot be saved," + \
                     " since it is not completely specified: %s" % self)

        # insert job
        try:

            # create entry in database
            status = db.insert(self)
            if status != 1:
                raise JobError("Cannot insert job %s" % self)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # update status
        self.existsInDataBase = True

        return status

    ##########################################################################

    def remove(self, db):
        """
        remove job object from database
        """

        # verify data is complete
        if not self.valid(['jobId', 'taskId']):
            raise JobError("The following job instance cannot be removed," + \
                     " since it is not completely specified: %s" % self)

        # remove from database
        try:
            status = db.delete(self)
            if status < 1:
                raise JobError("Cannot remove job %s" % self)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # update status
        self.existsInDataBase = False

        # return number of entries removed
        return status

    ##########################################################################

    def update(self, db, deep = True):
        """
        update job information in database
        """

        # verify if the object exists in database
        if not self.existsInDataBase:

            # no, use save instead of update
            return self.save(db)

        # verify data is complete
        if not self.valid(['jobId', 'taskId']):
            raise JobError("The following job instance cannot be updated," + \
                     " since it is not completely specified: %s" % self)

        # update it on database
        try:
            status = db.update(self)

            # update running job if associated
            if deep and self.runningJob is not None:

                status += self.runningJob.update(db)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # return number of entries updated.
        return status

   ##########################################################################

    def load(self, db, deep = True):
        """
        load information from database
        """

        # verify data is complete
        if not self.valid(['id']) and not self.valid(['name']) and \
           not self.valid(['jobId']):
            raise JobError("The following job instance cannot be loaded" + \
                     " since it is not completely specified: %s" % self)

        # get information from database based on template object
        try:
            objects = db.select(self)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # since required data is a key, it should be a single object list
        if len(objects) == 0:
            raise JobError("No job instances corresponds to the," + \
                     " template specified: %s" % self)

        if len(objects) > 1:
            raise JobError("Multiple job instances corresponds to the" + \
                     " template specified: %s" % self)

        # copy fields
        for key in self.fields:
            self.data[key] = objects[0][key]

        # get associated running job if it exists
        if deep:
            self.getRunningInstance(db)

        # update status
        self.existsInDataBase = True

    ##########################################################################

    def newRunningInstance(self, runningJob, db):
        """
        set currently running job
        """

        # close previous running instance (if any)
        self.closeRunningInstance(db)

        # update job id and submission counter
        runningJob['jobId'] = self.data['jobId']
        runningJob['taskId'] = self.data['taskId']
        self.data['submissionNumber'] += 1
        runningJob['submission'] = self.data['submissionNumber']
        runningJob.existsInDataBase = False
        
        # store instance
        self.runningJob = runningJob

    ##########################################################################

    def getRunningInstance(self, db):
        """
        get running job information
        """

        # create template
        template = RunningJob()
        template['jobId'] = self['jobId']
        template['taskId'] = self['taskId']
        template['closed'] = "N"

        # get running job
        runningJobs = db.select(template)

        # no running instance
        if runningJobs == []:
            self.runningJobs = []

        # one running instance
        elif len(runningJobs) == 1:
            self.runningJob = runningJobs[0]

        # oops, more than one!
        else:
            raise JobError("Multiple running instances of job %s : %s" % \
                           (self['jobId'], len(runningJobs)))

    ##########################################################################

    def closeRunningInstance(self, db):
        """
        close the running instance.
        it should be only one but ignore if there are more than one...
        """

        # create template
        template = RunningJob()
        template['jobId'] = self['jobId']
        template['taskId'] = self['taskId']
        template['closed'] = "N"

        # get running job
        runningJobs = db.select(template)

        # do for all of them (should be one...)
        for job in runningJobs:

            job["closed"] = "Y";
            job.update(db)






