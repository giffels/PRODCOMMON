#!/usr/bin/env python
"""
_SchedulerFake_
"""

__revision__ = "$Id: SchedulerFake.py,v 1.10 2009/06/09 13:46:01 gcodispo Exp $"
__version__ = "$Revision: 1.10 $"

import re, os
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob


    ##########################################################################
class SchedulerFake(SchedulerInterface) :
    """
    basic class to scheduler usage
    """
    def __init__( self, **args ):

        # call super class init method
        super(SchedulerFake, self).__init__(**args)

        self.cpCmd = args.get("cpCmd", 'cp')
        self.remoteDir  = args.get("remoteDir", '')
        

    # Generic static parameter, if needed
    delegationId = "bossproxy"
    SandboxDir = "SandboxDir"
    zippedISB  = "zippedISB.tar.gz"
    statusMap = {
        'PEND'  : 'SW',
        'RUN'   : 'R',
        'EXIT'  : 'SD',
        'PSUSP' : 'DA',
        'USUSP' : 'DA',
        'SSUSP' : 'DA',
        'UNKWN' : 'UN',
        'DONE'  : 'SD',
        'WAIT'  : 'SW',
        'ZOMBI' : 'DA'
        }


    ##########################################################################

    def submit( self, task, requirements='', config ='', service='' ):
        """
        user submission function
        
        takes as arguments:
        - a finite, dedicated jdl
        - eventually a list of services to connect
        - eventually a config file

        the passed config file or, if not provided, a default one can be
        used from eventual defaults

        the function returns an eventual parent id, the service of the
        successfully submission and a map associating the jobname to the
        node id. If the submission is not bulk, the parent id should be the
        node id of the unique entry of the map
        
        """

        taskId = None
        queue = None
        retMap = {}

        for job in task.jobs:
            command = self.decodeJob (job, task, requirements )
            out, ret = self.ExecuteCommand( command )
            if ret != 0 :
                raise SchedulerError('Error in submit', out, command )

            r = re.compile("Job <(\d+)> is submitted.*<(\w+)>")

            m = r.search(out)
            if m is not None:
                jobId = m.group(1)
                queue = m.group(2)
                retMap[job['name']] = jobId
            else:
                rNot = re.compile("Job not submitted.*<(\w+)>")
                m = rNot.search(out)
                if m is not None:
                    self.logging.error( "Job NOT submitted: %s" % out)
                    job.runningJob.errors.append(
                        'Cannot submit using %s: %s' % ( out, command )
                        )

        return retMap, taskId, queue


    ##########################################################################

    def getOutput( self, obj, outdir='' ):
        """
        retrieve output or just put it in the destination directory
        """

        # obj can be a task, a job or even a running job
        # several possibilities:
        # 1) connect to a service and perform a remote copy
        # 2) just eventually copy the local output to the destination dir
        # 3) wrap a CLI command like glite-wms-job-output

        errorList = []

        if outdir == '' and obj['outputDirectory'] is not None:
            outdir = obj['outputDirectory']

        if outdir != '' and not os.path.exists( outdir ) :
            raise SchedulerError( 'Permission denied', \
                                  'Unable to write files in ' + outdir )
                

        # retrieve scheduler id list
        schedIdList = {}
        for job in obj.jobs:
            if self.valid( job.runningJob ):
                # retrieve output
                # if error: job.runningJob.errors.append( error )
                pass

    ##########################################################################

    def kill( self, obj ):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        # several possibilities:
        # 1) connect to a service and perform a kill
        # 2) wrap a CLI command like glite-wms-job-cancel

        for job in obj.jobs:
            if self.valid( job.runningJob ):
                # kill
                # if error: job.runningJob.errors.append( error )
                pass


    ##########################################################################

    def purgeService( self, obj ):
        """
        Purge job (even bulk) from service
        """

        # not always available...
        # it may be useful to connect to a remote service and purge job sandbox
        
        out = "whatever" 
        if out.find( 'error' ) >= 0 :
            raise SchedulerError ( "Unable to purge job", out )


    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        resources list match
        """

        # several possibilities:
        # 1) connect to a service and ask
        # 2) wrap a CLI command like glite-wms-job-listmatch
        # 3) nor available... skip
        # 4) there is a useful lcgInfo...
        
        out = "whatever" 
        if out.find( 'error' ) >= 0 :
            raise SchedulerError ( "Unable to find resources", out )


    ##########################################################################

    def postMortem( self, obj, schedulerId, outfile, service):
        """
        perform scheduler logging-info
        
        """
        # here an actual example 
        
        command = "glite-wms-job-logging-info -v 2 " + schedulerId + \
                  " > " + outfile + "/gliteLoggingInfo.log"
            
        return self.ExecuteCommand( command, userProxy = self.cert )[0]


    ##########################################################################

    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        """

        # ask for the job informations, mainly status
        # some systems allow a query job per job, others also bulk queries

        #print schedIdList, service, objType
        r = re.compile("(\d+)\s+\w+\s+(\w+).*")
        rfull = re.compile("(\d+)\s+\w+\s+(\w+)\s+(\w+)\s+\w+\s+(\w+).*")
        rnotfound = re.compile("Job <(\d+)> is not found")
        for job in obj.jobs :

            if not self.valid( job.runningJob ) :
                continue

            jobid = str(job.runningJob['schedulerId']).strip()
            command = 'bjobs ' + str(jobid)
            out, ret = self.ExecuteCommand( command )
            if ret != 0 :
                raise SchedulerError('Error in status query', out, command )

            mnotfound = rnotfound.search(out)
            queue = None
            host = None
            sid = None
            st = None
            if (mnotfound):
                sid = mnotfound.group(1)
                st = 'DONE'
            else:
                mfull = rfull.search(out)
                if (mfull):
                    sid, st, queue, host = mfull.groups()
                else:
                    m = r.search(out)
                    if (m):
                        sid, st = m.groups()

            if (st) :
                job.runningJob['statusScheduler'] = st
                job.runningJob['status'] = self.statusMap[st]
            if (host):
                job.runningJob['destination'] = host



    ##########################################################################

    def jobDescription ( self, obj, requirements='', config='', service = '' ):

        """
        retrieve scheduler specific job description
        """

        # decode obj
        return self.decode( obj, requirements )

    ##########################################################################
    def decode  ( self, task, requirements='' ) :
        """
        prepare file for submission
        """

        ret = []
        for job in task.jobs:
            ret.append( self.decodeJob (job, task, requirements ) )

        return '\n'.join(ret)

    ##########################################################################
    def decodeJob (self, job, task, requirements='' ):
        """
        prepare file for submission
        """

        # prepare submission commanda
        command = 'cd ' + task['outputDirectory'] + "; "
        if job[ 'standardInput' ] != '':
            command += 'bsub -i %s ' % job[ 'standardInput' ]
        command += ' -o %s -e %s ' % \
                   ( job[ 'standardOutput' ], job[ 'standardError' ] )

        # blindly append user requirements
        command += requirements + " '"

        # buid up execution commands
        for inpFile in task[ 'globalSandbox' ].split(','):
            command += self.cpCmd + " " + \
                       self.remoteDir + "/" + inpFile + " .;"

        ## Job specific ISB
        for inpFile in job[ 'inputFiles' ]:
            if inpFile != '':
                command += self.cpCmd + " " + \
                           self.remoteDir + "/" + inpFile + " .;"

        ## set up the execution command
        command += "./" + os.path.basename( job[ 'executable' ] ) + \
                   " " + job[ 'arguments' ] + " ; "

        ## And finally copy back the output
        for outFile in job['outputFiles']:
            command += self.cpCmd + " " + outFile + " " + \
                   self.remoteDir + "/" + task['outputDirectory'] + "/. ; "

        command += "'"


        return command

