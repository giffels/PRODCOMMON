#!/usr/bin/env python
"""
_GLiteLBQuery_
GLite LB query functions
"""

__revision__ = "$Id: GLiteLBQuery.py,v 1.15 2008/09/09 08:40:14 gcodispo Exp $"
__version__ = "$Revision: 1.15 $"

from socket import getfqdn
from glite_wmsui_LbWrapper import Status
import Job as lbJob
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.Common.Exceptions import JobError



##########################################################################
class GLiteLBQuery(object):
    """
    basic class to handle glite jobs status query
    """

    
    statusMap = {
        'Undefined':'UN',
        'Submitted':'SU',
        'Waiting':'SW',
        'Ready':'SR',
        'Scheduled':'SS',
        'Running':'R',
        'Done':'SD',
        'Cleared':'E',
        'Aborted':'A',
        'Cancelled':'K',
        'Unknown':'UN',
        'Done(failed)':'DA',
        'Purged':'E'
        }

    statusList = [
        'Undefined',
        'Submitted',
        'Waiting',
        'Ready',
        'Scheduled',
        'Running',
        'Done',
        'Cleared',
        'Aborted',
        'Cancelled',
        'Unknown',
        'Done(failed)',
        'Purged'
        ]

    states = {}

    def __init__( self ):

        # counter
        self.st = 0

        # instatiating status object
        self.statusObj = Status()

        # Loading dictionary with available parameters list
        self.jobStatus = lbJob.JobStatus (self.statusObj)
        self.states = self.jobStatus.states_names
        self.attrNumber = self.jobStatus.ATTR_MAX

        # defining fields of interest
        self.status = self.states.index('Status')
        self.reason = self.states.index('Reason')
        self.networkServer = self.states.index('Network server')
        self.destination = self.states.index('Destination')
        self.stateEnterTimes = self.states.index('Stateentertimes')
        self.doneCode = self.states.index('Done code')
        self.jobId = self.states.index('Jobid')

        import re
        self.ft = re.compile("(\d+)Undefined=(\d+) Submitted=(\d+) Waiting=(\d+) Ready=(\d+) Scheduled=(\d+) Running=(\d+) Done=(\d+) Cleared=(\d+) Aborted=(\d+) Cancelled=(\d+) Unknown=(\d+) Purged=(\d+)")


    ##########################################################################
    def getJobInfo( self, jobInfo, runningJob ):
        """
        fill job dictionary with LB informations
        """

        if runningJob['statusScheduler'] == jobInfo[self.status]:
            return
        runningJob['statusScheduler'] = str(jobInfo[self.status])

        try:
            runningJob['statusReason'] = str(jobInfo[self.reason])
        except StandardError :
            pass

        try:
            wms = str( jobInfo[self.networkServer] )
            if wms != '' :
                wms = wms.replace( "https://", "" )
                tmp = wms.split(':')
                runningJob['service'] = \
                                 "https://" + getfqdn ( tmp[0] ) + ':' + tmp[1]
        except StandardError :
            pass

        try:
            destCe = str(jobInfo[self.destination])
            runningJob['destination'] = destCe.replace("https://", "")
            # runningJob['DEST_CE'] = \
            #                     destCe.split(':')[0].replace("https://", "")
        except StandardError :
            pass

        timestamp = str(jobInfo[self.stateEnterTimes])
        try:

            lst = self.ft.match( timestamp ).group( 6, 7, 8, \
                    self.statusList.index(runningJob['statusScheduler'])+2)
            
            if lst[0] == '0':
                try:
                    runningJob["scheduledAtSite"] = lst[0]
                except JobError, err:
                    pass
            if lst[1] != '0':
                runningJob["startTime"] = lst[1]
            if lst[2] != '0':
                runningJob["stopTime"] = lst[2]

            runningJob["lbTimestamp"] = lst[3]


            ###     pos = timestamp.find(runningJob['statusScheduler'])
            ###     runningJob["lbTimestamp"] = timestamp[
            ###         timestamp.find('=', pos)+1:timestamp.find(' ', pos)
            ###         ]
            ###     pos = timestamp.find('Scheduled')
            ###     Scheduled = timestamp[
            ###         timestamp.find('=', pos)+1:timestamp.find(' ', pos)
            ###         ]
            ###     if Scheduled  != '0':
            ###         print Scheduled
            ###     pos = timestamp.find('Running')
            ###     Running = timestamp[
            ###         timestamp.find('=', pos)+1:timestamp.find(' ', pos)
            ###         ]
            ###     if Running  != '0':
            ###         print Running

        except StandardError :
            pass

        try:
            if runningJob['statusScheduler'] == 'Done' \
                   and jobInfo[ self.doneCode ] != '0' :
                runningJob['statusScheduler'] = 'Done(failed)'
        except StandardError :
            pass

        runningJob['status'] = self.statusMap[runningJob['statusScheduler']]




    ##########################################################################
    def checkJobs( self, task, invalidList ):
        """
        check a list of provided job id

        return: map with key=gridid and the value is the map of jobs attributes
        """

        self.st = 0

        for job in task.jobs:

            # skip invalid entries
            if job.runningJob is None \
                   or job.runningJob.active != True \
                   or job.runningJob['schedulerId'] is None \
                   or job.runningJob['closed'] == "Y" \
                   or job.runningJob['status'] in invalidList :
                continue

            try:

                # convert to string
                jobid = str(job.runningJob['schedulerId']).strip()

                # load job info set
                self.statusObj.getStatus(jobid, 0)
                err, apiMsg = self.statusObj.get_error()
                if err :
                    job.error.append( "skipping " + jobid + " : " + apiMsg )
                    continue
                jobInfo = self.statusObj.loadStatus(self.st)

                # update runningJob
                self.getJobInfo(jobInfo, job.runningJob )

                self.st = self.st + 1

            except Exception, err :
                job.runningJob.warnings.append(
                    "skipping " + jobid + " : " +  str(err) )



    ##########################################################################
    def checkJobsBulk( self, task, jobIds, parentIds ):
        """
        check a list of provided job parent ids
        """

        if self.attrNumber == 0 :
            raise SchedulerError( 'Failed query', \
                                  'Problem loading jobStatus.ATTR_MAX' )
        self.st = 0

        for bulkId in parentIds:

            # convert to string
            bulkId = str(bulkId).strip()

            try:

                # load bulk info set
                self.statusObj.getStatus(bulkId, 0)
                err, apiMsg = self.statusObj.get_error()
                if err:
                    task.error.append( "skipping " + bulkId + " : " + apiMsg )
                    continue
                bulkInfo = self.statusObj.loadStatus(self.st)

                # how many jobs in the bulk? 
                intervals = int ( len(bulkInfo) / self.attrNumber )

                # loop over retrieved jobs
                for off in range ( 1, intervals ):

                    # adjust list ofset
                    offset = off * self.attrNumber

                    # focus over specific job related info
                    jobInfo = bulkInfo[ offset : offset + self.attrNumber ]

                    # retrieve scheduler Id
                    jobSchedId = str(jobInfo[self.jobId])

                    # update just loaded jobs
                    job = None
                    try:
                        job = task.jobs[ jobIds.pop(jobSchedId) ]
                    except :
                        continue

                    # update runningJob
                    self.getJobInfo( jobInfo, job.runningJob )

                self.st = self.st + 1

            except Exception, err :
                task.warnings.append("skipping " + bulkId + " : " +  str(err))




