#! /usr/bin/env python
"""
_CondorStatus_
Single, blocking, caching condor_q
"""

__revision__ = "$Id: CondorStatus.py,v 1.1.2.1 2009/11/19 23:30:10 ewv Exp $"
__version__ = "$Revision: 1.1.2.1 $"


import cStringIO
import os
import logging
import time
import threading

from socket import getfqdn
from xml.sax import make_parser
from xml.sax.handler import feature_external_ges

from WMCore.Algorithms.Singleton import Singleton
from CondorHandler import CondorHandler

# If seenJobs doesn't work because of race condition, could also try
# leave_in_queue = True
# periodic_remove = (JobStatus == 4) && ((CurrentTime - EnteredCurrentStatus) > (3600))

# Not seen a job should trigger query
# when executing query, should remove jobs from that schedd from jobDicts

class CondorStatus(Singleton):
    """
    A thread safe, multiple schedd, caching condor status object.
    Re-aquires status of jobs requested if over time out
    """

    def __init__(self, cacheTime = 60):
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


    def query(self, taskId = None, schedd = None, force = False):

        jobIds = {}
        bossIds = {}

        # FUTURE: Remove code for Condor < 7.3 when OK
        # FUTURE: look at -attributes to condor_q to limit the XML size. Faster on both ends
        self.condorqMutex.acquire()
        schedCmd = ''
        if schedd and schedd != self.hostname:
            schedCmd = ' -name ' + schedd
        else:
            schedd = self.hostname

        if force or (self.times.has_key(schedd) and
                     time.time()-self.times[schedd] < self.cacheTime):
            logging.debug("Cache for %s exists, returning" % schedd)
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
        self.condorqMutex.release()
        return self.jobDicts

if __name__ == '__main__' :
    logging.basicConfig(level=logging.DEBUG)

    def makeNew():
        s = CondorStatus()
        print s.query()

    # Check threading. Thread 2 should cache, thread 3 re-rerun condor_q

    print "Thread 1"
    threading.Thread(target = makeNew).start()
    print "Thread 2"
    threading.Thread(target = makeNew).start()
    time.sleep(70)
    print "Thread 3"
    threading.Thread(target = makeNew).start()

    # Check caching. 2nd invocation should re-run, third use cache

    s = CondorStatus()
    s.query(schedd='cmslpc05.fnal.gov')
    s.query(schedd='cmslpc06.fnal.gov')
    s.query(schedd='cmslpc05.fnal.gov')

