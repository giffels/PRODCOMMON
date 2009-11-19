#! /usr/bin/env python
"""
_CondorStatus_
Single, blocking, caching condor_q
"""

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.55.2.8 2009/11/19 20:55:04 ewv Exp $"
__version__ = "$Revision: 1.55.2.8 $"


import commands
import cStringIO
import os
import logging
import popen2
import re
import shutil
import time
import threading
# from set import Set

from socket import getfqdn
from xml.sax import make_parser
from xml.sax.handler import feature_external_ges

from WMCore.Algorithms.Singleton import Singleton
from CondorHandler import CondorHandler

# If this doesn't work, could also try
# leave_in_queue = True
# periodic_remove = (JobStatus == 4) && ((CurrentTime - EnteredCurrentStatus) > (3600))

# Not seen a job should trigger query
# when executing query, should remove jobs from that schedd from jobDicts

class CondorStatus(Singleton):


    def __init__(self, cacheTime = 15):
        super(CondorStatus, self).__init__()
        try:
            self.times
        except AttributeError:
            self.times = {}
            self.cacheTime = cacheTime
            self.hostname   = getfqdn()
            self.condorqMutex = threading.Lock()
            self.jobDicts = {}
            self.seenJobs = set()
            self.query()


    def query(self, taskId = None, schedd = None):

        jobIds = {}
        bossIds = {}

        # FUTURE: Remove code for Condor < 7.3 when OK
        # FUTURE: look at -attributes to condor_q to limit the XML size. Faster on both ends
        self.condorqMutex.acquire()
        print "Starting query",self.times
        schedCmd = ''
        if schedd and schedd != self.hostname:
            schedCmd = ' -name ' + schedd
        else:
            schedd = self.hostname

        if self.times.has_key(schedd) and time.time()-self.times[schedd] < self.cacheTime  :
            print "Cache for %s Already exists, bailing" % schedd, time.time()
            self.condorqMutex.release()
            return self.jobDicts

        self.times[schedd] = time.time()
        print "time is",self.times[schedd]
        cmd = 'condor_q -xml' + schedCmd

        if taskId:
            cmd += """ -constraint 'BLTaskID=?="%s"'""" % taskId
        print "Command is",cmd

        for jobId in self.jobDicts.keys():
            print "Checking %s agains %s",schedd,jobId
            if jobId.find(schedd) == 0:
                del self.jobDicts[jobId]
        (inputFile, outputFp) = os.popen4(cmd)

        try:
            xmlLine = ''
            while xmlLine.find('<?xml') == -1: # Throw away junk lines from condor < 7.3
                xmlLine = outputFp.readline()  # Remove when obsolete

            outputFile = cStringIO.StringIO(xmlLine+outputFp.read())
            # outputFile = cStringIO.StringIO(outputFp.read()) # Condor 7.3 version
        except:
            raise SchedulerError('Problem reading output of command', cmd)

        # If the command succeeded, close returns None
        # Otherwise, close returns the exit code
        if outputFp.close():
            raise SchedulerError("condor_q command or cache file failed.")

        handler = CondorHandler('GlobalJobId',
                    ['JobStatus', 'GridJobId','ProcId','ClusterId',
                    'MATCH_GLIDEIN_Gatekeeper', 'GlobalJobId'])
        parser = make_parser()
        try:
            parser.setContentHandler(handler)
            parser.setFeature(feature_external_ges, False)
            parser.parse(outputFile)
        except:
            raise SchedulerError('Problem parsing output of command', cmd)

        print  "Before update",self.jobDicts.keys()
        self.jobDicts.update(handler.getJobInfo())
        print  "After update",self.jobDicts.keys()
        [self.seenJobs.add(x) for x in self.jobDicts.keys()]
        time.sleep(10)
        self.condorqMutex.release()
        return self.jobDicts

def makeNew():
    s = CondorStatus()
    s.query()
#

#
print "Thread 1"
t1 = threading.Thread(target = makeNew).start()
print "Thread 2"
t2 = threading.Thread(target = makeNew).start()
time.sleep(20)
print "Thread 3"
t2 = threading.Thread(target = makeNew).start()

s = CondorStatus()
s.query(schedd='cmslpc05.fnal.gov')
s.query(schedd='cmslpc06.fnal.gov')
s.query(schedd='cmslpc05.fnal.gov')

# s1 = CondorStatus()
# # print id(s1), s1.internalId()
#
# s2 = CondorStatus()
# # print id(s2), s2.internalId()
#
# statusDict = s2.query()
# print statusDict
# print statusDict.keys()
# print statusDict.keys()[0]

# for i in xrange(100):
#     statusDict = s1.query()
#     print statusDict[statusDict.keys()[0]]