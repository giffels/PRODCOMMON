#!/usr/bin/env python
"""
BossLite exceptions
"""

__version__ = "$Id: Exceptions.py,v 1.5 2008/06/27 14:30:16 gcodispo Exp $"
__revision__ = "$Revision: 1.5 $"
__author__ = "Carlos.Kavka@ts.infn.it"

class JobError(Exception):
    """
    errors with jobs
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class TaskError(Exception):
    """
    errors with tasks
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class DbError(Exception):
    """
    MySQL, SQLLite and possible other exceptions errors are redirected to
    this exception type
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class SchedulerError(Exception):
    """
    scheduler errors
    """

    def __init__(self, value, message):
        self.value = value
        self.message = message

    def __str__(self):
        return self.value + '\n' + self.message

    def description(self):
        """
        returns a short description of the exception
        """

        return self.value

    def errorDump(self):
        """
        returns the original error message 
        """

        return self.message

class TimeOut(Exception):
    """
    operation timed out
    """

    def __init__(self, partialOut, value, start=None, stop=None):
        self.partialOut = partialOut
        self.value = value
        self.start = start
        self.stop = stop

    def __str__(self):
        return """
Command Timed Out after %d seconds
issued at %d
ended at %d
        """ % (self.value, self.start, self.stop )

    def commandOutput( self ) :
        """
        returns the partial output recorded before timeout
        """

        return self.partialOut





