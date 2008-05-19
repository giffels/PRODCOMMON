#!/usr/bin/env python
"""
BossLite exceptions
"""

__version__ = "$Id: Exceptions.py,v 1.1 2007/12/21 09:09:29 ckavka Exp $"
__revision__ = "$Revision: 1.1 $"
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
        return repr(self.value + ' \t ' + self.message)


