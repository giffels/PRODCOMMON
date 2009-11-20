#! /usr/bin/env python
"""
_CondorStatus_
Single, blocking, caching condor_q
"""

__revision__ = "$Id: CondorStatus.py,v 1.1.2.3 2009/11/20 14:51:43 ewv Exp $"
__version__ = "$Revision: 1.1.2.3 $"


import cStringIO
import logging
import os
import threading
import time

from socket import getfqdn
from xml.sax import make_parser
from xml.sax.handler import feature_external_ges

from CondorHandler import CondorHandler
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from WMCore.Algorithms.Singleton import Singleton

# If seenJobs doesn't work because of race condition, could also try
# leave_in_queue = True
# periodic_remove = (JobStatus == 4) && ((CurrentTime - EnteredCurrentStatus) > (3600))

class CondorStatus(Singleton):
    """
    A thread safe, multiple schedd, caching condor status object.
    Re-aquires status of jobs requested if over time out
    """

    def __init__(self, cacheTime = 60):
        super(CondorStatus, self).__init__()
        try:
            test = self.times
        except AttributeError:
            self.times = {}
            self.cacheTime = cacheTime
            self.hostname   = getfqdn()
            self.condorqMutex = threading.Lock()
            self.jobDicts = {}
            self.seenJobs = set([])
            self.query()


    def seenJobList(self):
        """
        Get the list of all jobs ever seen. May be useful for
        determining the difference between "Done" and not seen yet.
        """
        return list(self.seenJobs)


    def query(self, taskId = None, schedd = None, force = False):
        """
        Determine if we need more info from condor_q, get it
        """

        # FUTURE: Remove code for Condor < 7.3 when OK
        # FUTURE: look at -attributes to condor_q to limit the XML size.
        #         Faster on both ends
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
        cmd = 'condor_q -xml' + schedCmd

        if taskId:
            cmd += """ -constraint 'BLTaskID=?="%s"'""" % taskId
        logging.debug("Executing %s" % cmd)

        # Delete info from the schedd we are about to query
        for jobId in self.jobDicts.keys():
            if jobId.find(schedd) == 0:
                del self.jobDicts[jobId]

        (inputFile, outputFp) = os.popen4(cmd)

        # Throw away junk lines from condor < 7.3, remove when obsolete
        # outputFile = cStringIO.StringIO(outputFp.read()) # Condor 7.3 version
        try:
            xmlLine = ''
            while xmlLine.find('<?xml') == -1:
                xmlLine = outputFp.readline()
            outputFile = cStringIO.StringIO(xmlLine+outputFp.read())
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

        logging.debug("Jobs before update:\n%s", self.seenJobList())
        self.jobDicts.update(handler.getJobInfo())
        for jobId in self.jobDicts.keys():
            self.seenJobs.add(jobId)
        logging.debug("Jobs after update:\n%s", self.seenJobList())
        self.condorqMutex.release()
        return self.jobDicts

if __name__ == '__main__' :
    # Check threading and caching behavior. Use debug statements

    logging.basicConfig(level=logging.DEBUG)

    def makeNew():
        """
        Make called in thread mode to make new CondorStatus, print output
        """

        cs = CondorStatus()
        cs.query()

    # Check threading. Thread 2 should cache, thread 3 re-rerun condor_q

    print "Thread 1"
    threading.Thread(target = makeNew).start()
    print "Thread 2"
    threading.Thread(target = makeNew).start()
    time.sleep(70)
    print "Thread 3"
    threading.Thread(target = makeNew).start()

    # Check caching. 2nd invocation should re-run, third use cache

    CS = CondorStatus()
    CS.query(schedd='cmslpc05.fnal.gov')
    CS.query(schedd='cmslpc06.fnal.gov')
    CS.query(schedd='cmslpc05.fnal.gov')

