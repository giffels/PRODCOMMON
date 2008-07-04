#!/usr/bin/env python
"""
BossLite logging facility
"""

__version__ = "$Id: BossLiteLogger.py,v 1.0 2008/06/30 15:41:04 gcodispo Exp $"
__revision__ = "$Revision: 1.0 $"
__author__ = "Giuseppe.Codispoti@cern.ch"

from ProdCommon.BossLite.Common.Exceptions import BossLiteError
from ProdCommon.BossLite.Common.Exceptions import TimeOut

class BossLiteLogger(object):
    """
    logs informations from the task and eventual exception raised
    """

    # default values for fields
    defaults = [ 'errors', 'warnings', 'type', 'description', \
                 'affectedJobs', 'message', 'partialOutput' ]

    def __init__(self, task=None, exception=None):
        """
        __init__
        """

        self.data = {}
        self.data['type'] = 'log'
        errors = []
        warnings = []
        affectedJobs = []
        wAffectedJobs = []

        # handle di exception
        if isinstance( exception, BossLiteError ) :
            self.data['type'] = 'error'
            self.data['description'] = exception.value
            self.data['message'] = exception.message()
            if isinstance( exception, TimeOut ) :
                self.data['partialOutput'] = exception.commandOutput()

        if task is not None :
            for job in task.jobs:
                # evaluate errors
                if job.runningJob.isError() :
                    errors.append( job.runningJob.errors )
                    affectedJobs.append( job['jobId'] )
                    
                # evaluate warning
                if job.runningJob.warnings != [] :
                    warnings.append( job.runningJob.warnings )
                    wAffectedJobs.append( job['jobId'] )


        if self.data['type'] == 'log' :
            if warnings != []:
                self.data['type'] = 'warning'
                self.data['affectedJobs'] = wAffectedJobs
            
            if errors != [] :
                self.data['type'] = 'error'
                self.data['affectedJobs'] = affectedJobs

    def __getitem__(self, field):
        """
        return one of the fields (in a dictionary form)
        """

        # get mapped field name
        return self.data[field]


    def __setitem__(self, field, value):
        """
        set one of the fields (in a dictionary form)
        """

        # set mapped field name
        if field in self.defaults:
            self.data[field] = value
            return

        # not there
        raise KeyError(field)


    def __str__(self):
        """
        return a printed representation of the situation
        """

        # get field names
        fields = self.data.keys()
        fields.sort()

        # the object can be empty unless for the type: return an empty string
        if len( fields ) == 1:
            return ''

        # show id first
        string = "Log Event type : %s \n" % self.data['type']
        fields.remove('type')

        # add the other fields
        for key in fields:
            string += "   %s : %s\n" % (str(key), str(self.data[key]))

        # return it
        return string
