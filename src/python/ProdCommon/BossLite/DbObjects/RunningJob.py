#!/usr/bin/env python
"""
_RunningJob_

"""

__version__ = "$Id: RunningJob.py,v 1.2 2007/12/21 11:08:55 ckavka Exp $"
__revision__ = "$Revision: 1.2 $"
__author__ = "Carlos.Kavka@ts.infn.it"

from ProdCommon.BossLite.DbObjects.DbObject import DbObject
from ProdCommon.BossLite.Common.Exceptions import JobError, DbError

class RunningJob(DbObject):
    """
    RunningJob object
    """

    # fields on the object and their names on database
    fields = { 'id' : 'id',
               'jobId' : 'job_id',
               'taskId' : 'task_id',
               'submission' : 'submission',
               'submissionPath' : 'submission_path',
               'scheduler' : 'scheduler',
               'service' : 'service',
               'schedulerId' : 'scheduler_id',
               'schedulerParentId' : 'scheduler_parent_id',
               'statusScheduler' : 'status_scheduler',
               'status' : 'status',
               'statusReason' : 'status_reason',
               'statusHistory' : 'status_history',
               'destination' : 'destination',
               'fileBlock' : 'file_block',
               'dlsDestination' : 'dls_destination',
               'lbTimestamp' : 'lb_timestamp',
               'submissionTime' : 'submission_time',
               'startTime' : 'start_time',
               'stopTime' : 'stop_time',
               'getOutputTime' : 'getoutput_time',
               'executionHost' : 'execution_host',
               'executionPath' : 'execution_path',
               'executionUser' : 'execution_user',
               'applicationReturnCode' : 'application_return_code',
               'wrapperReturnCode' : 'wrapper_return_code',
               'schedulerAttributes' : 'sched_attr',
               'closed' : 'closed'
             }

    # mapping between field names and database fields including superclass
    mapping = fields

    # default values for fields
    defaults = { 'id' : None,
                 'jobId' : None,
                 'taskId' : None,
                 'submission' : None,
                 'submissionPath' : None,
                 'scheduler' : None,
                 'service' : None,
                 'schedulerId' : None,
                 'schedulerParentId' : None,
                 'statusScheduler' : None,
                 'status' : None,
                 'statusReason' : None,
                 'statusHistory' : [],
                 'destination' : None,
                 'fileBlock' : None,
                 'dlsDestination' : None,
                 'lbTimestamp' : None,
                 'submissionTime' : None,
                 'startTime' : None,
                 'stopTime' : None,
                 'getOutputTime' : None,
                 'executionHost' : None,
                 'executionPath' : None,
                 'executionUser' : None,
                 'applicationReturnCode' : None,
                 'wrapperReturnCode' : None,
                 'schedulerAttributes' : None,
                 'closed' : None
               }

    # database properties
    tableName = "bl_runningjob"
    tableIndex = ["taskId", "jobId", "submission"]

    # exception class
    exception = JobError

    ##########################################################################

    def __init__(self, parameters = {}):
        """
        initialize a RunningJob instance
        """

        # call super class init method
        super(RunningJob, self).__init__(parameters)

    ##########################################################################

    def save(self, db):
        """
        save running job object in database. checking that static information
        is automatically performed due to database constraints
        """

        # verify data is complete
        if not self.valid(['submission', 'jobId', 'taskId']):
            raise JobError("The following job instance cannot be saved," + \
                     " since it is not completely specified: %s" % self)

        # insert running job
        try:

            # create entry in database
            status = db.insert(self)
            if status != 1:
                raise JobError("Cannot insert running job %s" % str(self))

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
        if not self.valid(['submission', 'jobId']):
            raise JobError("The following job instance cannot be removed," + \
                     " since it is not completely specified: %s" % self)

        # remove from database
        try:
            status = db.delete(self)
            if status < 1:
                raise JobError("Cannot remove running job %s" % str(self))

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
        if not self.valid(['submission', 'jobId', 'taskId']):
            raise JobError("The following job instance cannot be updated," + \
                     " since it is not completely specified: %s" % self)

        # update it on database
        try:
            status = db.update(self)
            if status < 1:
                raise JobError("Cannot update job %s" % str(self))

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # return number of entries updated.
        # since (submission + jobId) is a key,it will be 0 or 1
        return status

   ##########################################################################

    def load(self, db, deep = True):
        """
        load information from database
        """

       # verify data is complete
        if not self.valid(['name']):
            raise JobError("The following running job instance cannot be" + \
                     " loaded since it is not completely specified: %s" % self)

        # get information from database based on template object
        try:
            objects = db.select(self)

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # since required data is a key, it should be a single object list
        if len(objects) == 0:
            raise JobError("No running job instances corresponds to the," + \
                     " template specified: %s" % self)

        if len(objects) > 1:
            raise JobError("Multiple running job instances corresponds to" + \
                     " the template specified: %s" % self)

        # copy fields
        for key in self.fields:
            self.data[key] = objects[0][key]

       # update status
        self.existsInDataBase = True
