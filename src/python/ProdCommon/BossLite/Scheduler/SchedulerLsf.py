#!/usr/bin/env python
"""
basic LSF CLI interaction class
"""

__revision__ = "$Id: SchedulerLsf.py,v 1.3 2008/04/28 07:37:42 spiga Exp $"
__version__ = "$Revision: 1.3 $"

import re,os

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerLsf (SchedulerInterface) :
    """
    basic class to handle lsf jobs
    """
    def __init__( self, user_proxy = '' ):

        # call super class init method
        super(SchedulerLsf, self).__init__(user_proxy)
        self.statusMap = {
            'Undefined':'UN',
            'Submitted':'SU',
            'Waiting':'SW',
            'PEND':'SW',
            'Ready':'SR',
            'Scheduled':'SS',
            'Running':'R',
            'RUN':'R',
            'DONE':'SD',
            'Cleared':'E',
            'EXIT':'DA',
            'Aborted':'A',
            'Cancelled':'K',
            'Unknown':'UN',
            'Done(failed)':'DA'   
            }


    def checkUserProxy( self, cert='' ):
        return

    def jobDescription ( self, obj, requirements='', config='', service = '' ):
        """
        retrieve scheduler specific job description
        return it as a string
        """

    def submit ( self, obj, requirements='', config='', service = '' ) :
        """
        set up submission parameters and submit

        return jobAttributes, bulkId, service

        - jobAttributs is a map of the format
              jobAttributes[ 'name' : 'schedulerId' ]
        - bulkId is an eventual bulk submission identifier
        - service is a endpoit to connect withs (such as the WMS)
        """

        if type(obj) == RunningJob or type(obj) == Job:
            return self.submitJob(obj, requirements)
        elif type(obj) == Task :
            return self.submitTask (obj, requirements ) 

    def submitTask ( self, task, requirements=''):
        ret_map={}
        for job in task.getJobs() :
            map, taskId, queue = self.submitJob(job, task, requirements)
            ret_map.update(map)

        return ret_map, taskId, queue

    def submitJob ( self, job, task=None, requirements=''):
        """ Need to copy the inputsandbox to WN before submitting a job"""

        txt = "'"
        # Need to copy InputSandBox to WN
        if task:
            subDir=task[ 'startDirectory' ]
            for inpFile in task[ 'globalSandbox' ].split(','):
                txt += "cp "+subDir+"/"+inpFile+" . ; "
        ## Job specific ISB
        for inpFile in job[ 'inputFiles' ]:
            if inpFile != '': txt += "cp "+inpFile+" .;"

        ## Now the actual wrapper
        args = job[ 'arguments' ]
        exe = job[ 'executable' ]
        txt += "./"+os.path.basename(exe)+" "+args+" ; "

        ## And finally copy back the output
        outputDir=task['outputDirectory']
        for outFile in job['outputFiles']:
            txt += "cp "+outFile+" "+outputDir+". ; "

        txt += "'"

        arg = ""
        if job[ 'standardInput' ] != '':
            arg += ' -i %s ' % job[ 'standardInput' ]
        arg += ' -o %s ' % job[ 'standardOutput' ]
        arg += ' -e %s ' % job[ 'standardError' ]

        # blindly append user requirements
        arg += requirements

        # and finally the wrapper
        arg +=  '  %s ' % txt

        command = "bsub " + arg 

        out = self.ExecuteCommand(command)
        r = re.compile("Job <(\d+)> is submitted.*<(\w+)>")

        m= r.search(out)
        if m is not None:
            #taskId =m.group(1)
            queue = m.group(2)
        else:
            rNot = re.compile("Job not submitted.*<(\w+)>")
            m= rNot.search(out)
            if m is not None:
                print m
                print "Job NOT submitted"
                print out
            raise (out)
        taskId = None 
        #print "Your job identifier is: ", taskId, queue
        map={ job['name'] : taskId }
        return map, taskId, queue

    def query(self, schedIdList, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        It may use single 'node' scheduler id or bulk id for association
        
        return jobAttributes

        where jobAttributes is a map of the format:
           jobAttributes[ schedId :
                                    [ key : val ]
                        ]
           where key can be any parameter of the Job object and at least status
                        
        """
        ret_map={}
        #print schedIdList, service, objType
        r = re.compile("(\d+)\s+\w+\s+(\w+).*")
        rfull = re.compile("(\d+)\s+\w+\s+(\w+)\s+(\w+)\s+\w+\s+(\w+).*")
        rnotfound = re.compile("Job <(\d+)> is not found")
        for jobid in schedIdList:
            jobid = jobid.strip()
            cmd='bjobs '+str(jobid)
            #print cmd
            out = self.ExecuteCommand(cmd)
            #print "<"+out+">"
            mnotfound= rnotfound.search(out)
            queue=None
            host=None
            id=None
            st=None
            if (mnotfound):
                id=mnotfound.group(1)
                st='Done'
            else:
                mfull= rfull.search(out)
                if (mfull):
                    id,st,queue,host=mfull.groups()
                else:
                    m= r.search(out)
                    if (m):
                        id,st=m.groups()
                    pass
                pass
            pass

            #print id,st,queue,host

            map={}
            if (st) :
                map['statusScheduler']=st
                map['status'] = self.statusMap[st]
            if (host): map['destination']=host
            ret_map[jobid]=map
        return ret_map


    def getOutput( self, schedIdList, outdir, service ):
        """
        retrieve output or just put it in the destination directory

        does not return
        """
        #it should just move the output form where has been placed by LSF to the outdir location


    def kill( self, schedIdList, service ):
        """
        kill the job instance

        does not return
        """
        r = re.compile("Job <(\d+)> is being terminated")
        rFinished = re.compile("Job <(\d+)>: Job has already finished")
        for jobid in schedIdList:
            jobid = jobid.strip()
            cmd='bkill '+str(jobid)
            out = self.ExecuteCommand(cmd)
            mFailed= rFinished.search(out)
            if mFailed:
                raise SchedulerError ( "Unable to kill job "+jobid+" . Reason: ", out )
            pass
        pass


    def postMortem ( self, schedIdList, outfile, service ) :
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """


    def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):
        """
        perform a resources discovery
        returns a list of resulting sites
        """

        return  seList
