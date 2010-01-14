#!/usr/bin/env python
"""
_GLiteLBQuery_
GLite LB query functions
"""

__revision__ = "$Id: GLiteStatusQuery.py,v 1.5 2009/12/17 16:31:13 spigafi Exp $"
__version__ = "$Revision: 1.5 $"

import sys
from socket import getfqdn
from glite_wmsui_LbWrapper import Status
import wmsui_api
from copy import deepcopy
import getopt
import simplejson as json


##########################################################################
class GLiteStatusQuery(object):
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

        # Loading dictionary with available parameters list
        self.states = wmsui_api.states_names
        self.attrNumber = wmsui_api.STATE_ATTR_MAX

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
    def getJobInfo( self, jobInfo, runningJob, forceAborted=False ):
        """
        fill job dictionary with LB informations
        """

        if forceAborted and self.statusMap[jobInfo[self.status]] == 'SU' :
            runningJob['statusScheduler'] = 'Aborted'
        elif runningJob['status'] == self.statusMap[jobInfo[self.status]]:
            return
        else:
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
            
            if lst[0] != '0':
                try:
                    runningJob["scheduledAtSite"] = lst[0]
                except KeyError :
                    pass
                
            if lst[1] != '0':
                runningJob["startTime"] = lst[1]
                
            if lst[2] != '0':
                runningJob["stopTime"] = lst[2]
                
            if lst[3] != '0':
                runningJob["lbTimestamp"] = lst[3]

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
    
    def checkJobs( self, jobIds, errors ):
        """
        check a list of provided job id
        """
        
        for jobId in jobIds:

            try:
                
                wrapStatus = Status(jobId, 0)
                # Check for Errors
                err , apiMsg = wrapStatus.get_error ()
                if err:
                    # Print the error and terminate
                    raise Exception(apiMsg)

                # Retrieve the number of status (in this case is always 1)
                # statusNumber = wrapStatus.getStatusNumber()
 
                # Retrieve the list of attributes for the current UNIQUE event
                statusAttribute = wrapStatus.getStatusAttributes(0)
                
                # Check for Errors
                err , apiMsg = wrapStatus.get_error ()
                if err:
                    # Print the error and terminate
                    raise Exception(apiMsg)

                jobInfo = statusAttribute

                # retrieve scheduler Id
                jobSchedId = str(jobInfo[self.jobId])

                # update just loaded jobs
                try:
                    job = jobIds[jobSchedId]
                except :
                    continue
                    
                 # update runningJob
                self.getJobInfo(jobInfo, job )
                
                jobIds[jobSchedId] = job
                
            except Exception, err :
                errors.append(
                    "skipping " + jobId + " : " +  str(err) )
       
    ##########################################################################
    
    def checkJobsBulk( self, jobIds, parentIds, errors ):
        """
        check a list of provided job parent ids
        """

        if self.attrNumber == 0 :
            # raise an exception here? What kind of exception?
            raise 
        
        self.st = 0

        # convert to string
        # lbJobs = wmsui_api.getJobIdfromList ( parentIds )

        for bulkId in parentIds:
            
            try:

                wrapStatus = Status(bulkId, 0)
                # Check for Errors
                err , apiMsg = wrapStatus.get_error ()
                if err:
                    # Print the error and terminate
                    raise Exception(apiMsg)

                # Retrieve the number of status
                statesNumber = wrapStatus.getStatusNumber()

                # Retrieve the list of attributes for each logged event
                for statusNumber in range(statesNumber):

                    # Retrieve the list of attributes for the current event
                    statusAttribute = \
                        wrapStatus.getStatusAttributes(statusNumber)
                    
                    # Check for Errors
                    err , apiMsg = wrapStatus.get_error ()
                    if err:
                        # Print the error and terminate
                        raise Exception(apiMsg)

                bulkInfo = statusAttribute
                
                # how many jobs in the bulk?
                intervals = int ( len(bulkInfo) / self.attrNumber )

                # look if the parent is aborted
                if str(bulkInfo[self.status]) == 'Aborted' :
                    forceAborted = True
                    errors.append('Parent Job Failed')
                else :
                    forceAborted = False

                # loop over retrieved jobs
                for off in range ( 1, intervals ):

                    # adjust list ofset
                    offset = off * self.attrNumber

                    # focus over specific job related info
                    jobInfo = bulkInfo[ offset : offset + self.attrNumber ]

                    # retrieve scheduler Id
                    jobSchedId = str(jobInfo[self.jobId])

                    # update just loaded jobs
                    #job = None
                    try:
                        job = jobIds[jobSchedId]
                    except :
                        continue

                    # update runningJob
                    self.getJobInfo( jobInfo, job, forceAborted)
                    jobIds[jobSchedId] = job
                    

                self.st = self.st + 1

            except Exception, err :
                errors.append("skipping " + bulkId + " : " +  str(err))


def usage():
    """
    print out help
    """
    usageStr = ''' 
    python GLiteStatusQuery.py [-p <parentId> -j <jobId1,jobId2,...,jobIdN>][-f <infile>] [-o <outfile>] [-h]
    Options:
    -h|--help        print this summary
    -f|--file=       input file in json format
    -o|--output=     redirect output to file location 
    -p|--parentId=   id for the collection 
    -j|--jobId=      list of job ids (comma separated)
    '''
        
    return usageStr 

def main():
    """
    __main__
    """

    # parse options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   "", ["help", "file=", "output=", 
                                        "parentId=", "jobId="])
    except getopt.GetoptError, err:
        print json.dumps({'statusQuery': [], 
                           'errors' : [ str(err)+" "+usage() ]})
        sys.exit(2)

    inputFile = None 
    outputFile = None
    parent = []
    jobList = []

    for o, a in opts:
        
        if o in ("-h", "--help"):
            print usage()
            # print json.dumps( {'statusQuery': [], 'errors' : [ usage() ]} )
            sys.exit()
        elif o in ("-f", "--file"):
            inputFile = a
        elif o in ("-o", "--output"):
            outputFile = a
        elif o in ("-p", "--parentId"):
            parent = a.split(',')
        elif o in ("-j", "--jobId"):
            jobList = a.split(',')
        else:
            print json.dumps({'statusQuery': [], 
                               'errors' : [ "Unknown parameter."]})
            sys.exit(2)

    if inputFile:
        try:
            fp = open(inputFile, "r")
            inputDict = json.load(fp)
            parent = inputDict['parentId']
            jobList = inputDict['jobIdList']
            fp.close()
        except Exception, ex:
            print json.dumps({'statusQuery': [],
                              'errors' : [ "inputFile in unexpected format." ]})
            sys.exit(2)
        
    elif len(jobList)==0:
        print json.dumps({'statusQuery': [],
                          'errors' : [ "At least one jobId is needed." ]})
        sys.exit(2)

    # LB data structures 
    template = { 'id' : None,
                 'jobId' : None,
                 'taskId' : None,
                 'schedulerId' : None,
                 'schedulerParentId' : None,
                 'statusScheduler' : None,
                 'status' : None,
                 'statusReason' : None,
                 'destination' : None,
                 'lbTimestamp' : None,
                 'scheduledAtSite' : None,
                 'startTime' : None,
                 'stopTime' : None
               }

    # jobId for re-mapping
    jobIds = {}

    # errors list
    errors = []

    # loop!
    for job in jobList :

        rJob = deepcopy(template)
        rJob['schedulerId'] = job

        # append in job list
        jobIds[ job ] = rJob

    lbInstance = GLiteStatusQuery()
    
    if parent :
        lbInstance.checkJobsBulk( jobIds, parent, errors )     
    else :
        lbInstance.checkJobs( jobIds, errors )
    
    # printout JSON formatted list of status records with errors
    outDict = {
        'statusQuery': jobIds.values(), 
        'errors' : errors,   
    }

    if outputFile:
        try:
            fp = open(outputFile, 'w')
            json.dump(outDict, fp)
            fp.close()
        except Exception, ex:
            print json.dumps( {'statusQuery': [], 'errors' : [ str(ex) ]} )
            sys.exit(2)
    else:
        print json.dumps(outDict)


########## END MAIN ##########

if __name__ == "__main__":
    main()

