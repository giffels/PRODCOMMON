#!/usr/bin/env python
"""
BossLite exceptions
"""

__version__ = "$Id$"
__revision__ = "$Revision$"
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
        return repr(self.value + self.message)


