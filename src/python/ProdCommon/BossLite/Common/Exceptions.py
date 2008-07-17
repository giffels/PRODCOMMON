#!/usr/bin/env python
"""
BossLite exceptions
"""

__version__ = "$Id: Exceptions.py,v 1.7 2008/07/04 10:13:54 gcodispo Exp $"
__revision__ = "$Revision: 1.7 $"
__author__ = "Carlos.Kavka@ts.infn.it"
import inspect

class BossLiteError(Exception):
    """
    errors base class
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(BossLiteError, self).__init__(value)
        Exception.__init__( self  )
        self.value = self.__class__.__name__
        self.msg = value
        stack = inspect.trace(1)[-1]
        self.data = { 'FileName' : stack[1],
                      'LineNumber' : stack[2],
                      'MethodName' : stack[3],
                      'LineContent' : stack[4] }


    def __str__(self):
        """
        __str__
        """

        return repr(self.msg)

    def message(self):
        """
        error description
        """

        return self.msg
    

class JobError(BossLiteError):
    """
    errors with jobs
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(JobError, self).__init__(value)
        BossLiteError.__init__( self, value )


class TaskError(BossLiteError):
    """
    errors with tasks
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(TaskError, self).__init__(value)
        BossLiteError.__init__( self, value )


class DbError(BossLiteError):
    """
    MySQL, SQLite and possible other exceptions errors are redirected to
    this exception type
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(DbError, self).__init__(value)
        BossLiteError.__init__( self, value )

class SchedulerError(BossLiteError):
    """
    scheduler errors
    """

    def __init__(self, value, msg):
        """
        __init__
        """

        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(SchedulerError, self).__init__(value)
        BossLiteError.__init__(self, value)
        self.value = value
        self.msg = msg

    def __str__(self):
        """
        __str__
        """

        return self.value + '\n' + self.msg

    def description(self):
        """
        returns a short description of the exception
        """

        return self.value

    def errorDump(self):
        """
        returns the original error message 
        """

        return self.msg


class TimeOut(BossLiteError):
    """
    operation timed out
    """

    def __init__(self, partialOut, value, start=None, stop=None):
        """
        __init__
        """

        self.partialOut = partialOut
        self.timeout = value
        self.start = start
        self.stop = stop
        self.value = \
              "Command Timed Out after %d seconds, issued at %d, ended at %d" \
              % (self.timeout, self.start, self.stop )
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(TimeOut, self).__init__(self.__str__())
        BossLiteError.__init__(self, self.value)

    def commandOutput( self ) :
        """
        returns the partial output recorded before timeout
        """

        return self.partialOut









