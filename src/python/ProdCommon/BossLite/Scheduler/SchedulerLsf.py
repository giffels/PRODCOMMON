#!/usr/bin/env python
"""
basic LSF CLI interaction class
"""

__revision__ = "$Id: SchedulerLsf.py,v 1.21 2008/12/01 11:55:57 spiga Exp $"
__version__ = "$Revision: 1.21 $"

import re, os, time
import tempfile

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerLsf (SchedulerInterface) :
    """
    basic class to handle lsf jobs
    """
    def __init__( self, **args):

        # call super class init method
        super(SchedulerLsf, self).__init__(**args)
        self.statusMap = {
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
        self.cpCmd  = 'cp'
        self.rfioSer= '' 
        try:
            self.cpCmd  =args['cpCmd']
            self.rfioSer=args['rfioSer'] 
        except:
            pass
        self.ksuCmd = '' 
        if self.cert != '':
            self.ksuCmd = 'cd /tmp; unset LD_LIBRARY_PATH; export PATH=/usr/bin:/bin; source /etc/profile; '
            self.userToken = args.get('userToken','')

    def checkUserProxy( self, cert='' ):
        """ 
        to be implemented based on token....
        """ 
        return

    def jobDescription ( self, obj, requirements='', config='', service = '' ):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        args='' 
        if type(obj) == RunningJob or type(obj) == Job:
            return self.decode(obj, requirements)
        elif type(obj) == Task :
            task = obj
            for job in task.getJobs() :
                args += self.decode(job, task, requirements)+'  \n'
            return args 


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

        if self.ksuCmd : self.storeToken()  
 
        ret_map={}
        for job in task.getJobs() :
            map, taskId, queue = self.submitJob(job, task, requirements)
            ret_map.update(map)

        return ret_map, taskId, queue

    def submitJob ( self, job, task=None, requirements=''):
        """ Need to copy the inputsandbox to WN before submitting a job"""

        arg = self.decode(job, task, requirements )

        # command = "bsub " + arg 
        # execute bsub in the directory where files have be returned
        chDir = "pushd . > /dev/null; cd " + task['outputDirectory'] + "; "
        resetDir = " ; popd > /dev/null"
        command = chDir + "bsub " + arg + resetDir 
        
        if self.ksuCmd :   
            # token stuff are a temporary patch
            cmd = "/afs/usr/local/etc/SetToken < %s\n"%self.userToken 
            cmd += '%s\n'%command
            cmd += "/afs/usr/local/etc/SetToken < %s\n"%self.serverToken 
            command,fname = self.createCommand(cmd, task)

        out, ret = self.ExecuteCommand( command )
        if self.ksuCmd: os.unlink( fname )
        if ret != 0 :
            raise SchedulerError('Error in submit', out, command )

        r = re.compile("Job <(\d+)> is submitted.*<(\w+)>")

        m= r.search(out)
        if m is not None:
            jobId =m.group(1)
            queue = m.group(2)
        else:
            rNot = re.compile("Job not submitted.*<(\w+)>")
            m= rNot.search(out)
            if m is not None:
                print m
                print "Job NOT submitted"
                print out
            raise SchedulerError('Cannot submit ', out, command)
        taskId = None 
        #print "Your job identifier is: ", taskId, queue
        map={ job['name'] : jobId }
        return map, taskId, queue


    def decode (self, job, task=None, requirements='' , config ='', service='' ):
        """
        prepare file for submission
        """

        txt = "'"
        # Need to copy InputSandBox to WN
        if task:
            #subDir=task[ 'startDirectory' ]
            for inpFile in task[ 'globalSandbox' ].split(','):
                txt += self.cpCmd+" "+self.rfioSer+"/"+inpFile+" . ; "
        ## Job specific ISB
        for inpFile in job[ 'inputFiles' ]:
            if inpFile != '': txt += self.cpCmd+" "+self.rfioSer+"/"+inpFile+" .;"

        ## Now the actual wrapper
        args = job[ 'arguments' ]
        exe = job[ 'executable' ]
        txt += "./"+os.path.basename(exe)+" "+args+" ; "

        ## And finally copy back the output
        outputDir=task['outputDirectory']
        for outFile in job['outputFiles']:
            txt += self.cpCmd+" "+outFile+" "+self.rfioSer+"/"+outputDir+"/. ; "

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

        return arg 



    ##########################################################################
    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        It may use single 'node' scheduler id or bulk id for association
        """
        if type(obj) != Task :
            raise SchedulerError('wrong argument type', str( type(obj) ))


        #print schedIdList, service, objType
        r = re.compile("(\d+)\s+\w+\s+(\w+).*")
        rfull = re.compile("(\d+)\s+\w+\s+(\w+)\s+(\w+)\s+\w+\s+(\w+).*")
        rnotfound = re.compile("Job <(\d+)> is not found")
        for job in obj.jobs :

            if not self.valid( job.runningJob ) :
                continue

            jobid = str(job.runningJob['schedulerId']).strip()
            command='bjobs '+str(jobid)
            
            if self.ksuCmd :   
                # write a ksu tmpFile
                cmd = '%s\n'%command
                command,fname = self.createCommand(cmd, obj)

            out, ret = self.ExecuteCommand( command )
            if self.ksuCmd: os.unlink( fname )
            if ret != 0 :
                raise SchedulerError('Error in status query', out,command )
            #print "<"+out+">"
            mnotfound= rnotfound.search(out)
            queue=None
            host=None
            id=None
            st=None
            if (mnotfound):
                id=mnotfound.group(1)
                st='DONE'
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

            if (st) :
                job.runningJob['statusScheduler']=st
                job.runningJob['status'] = self.statusMap[st]
            if (host): job.runningJob['destination']=host



    def getOutput( self, schedIdList, outdir ):
        """
        retrieve output or just put it in the destination directory

        does not return
        """
        #it should just move the output form where has been placed by LSF to the outdir location


    def kill( self, obj ):
        """
        kill the job instance

        does not return
        """
        r = re.compile("Job <(\d+)> is being terminated")
        rFinished = re.compile("Job <(\d+)>: Job has already finished")
        # for jobid in schedIdList:
        for job in obj.jobs:
            if not self.valid( job.runningJob ):
                continue
            jobid = str( job.runningJob['schedulerId'] ).strip()
            command='bkill '+str(jobid)
            if self.ksuCmd :   
                # write a ksu tmpFile
                cmd = '%s\n'%command
                command,fname = self.createCommand(cmd, obj)

            out, ret = self.ExecuteCommand( command )
            if self.ksuCmd: os.unlink( fname )
            mFailed= rFinished.search(out)
            if mFailed:
                raise SchedulerError ( "Unable to kill job "+jobid+" . Reason: ", out, command )
            pass
        pass


    def postMortem ( self, schedIdList, outfile, service ) :
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """


#### Ksu Related Stuff
    def createCommand(self, cmd, obj):
        """ 
        write a ksu tmpFile
        """ 
        userName = obj['name'].split('_')[0] ## will be in the DB
        BaseCmd = self.ksuCmd +'/usr/kerberos/bin/ksu %s -k -c FILE:%s < '%( userName ,self.cert)

        tmp, fname = tempfile.mkstemp( "", "ksu_", os.getcwd() )
        os.close( tmp )
        tmpFile = open( fname, 'w')
        tmpFile.write( cmd )
        tmpFile.close()

        #redefine command
        command = BaseCmd + fname

        return command, fname
   
    def storeToken( self ):
        """
        This is a temporary method to 
        allow server at caf working.... TO DEPRECATE..
        (check if a valid token has already stored. If not 
         or if older than 12h, recreate it.)   
        """
        # token stuff are a temporary patch
        store = True
        self.serverToken = '/tmp/Token_Server'
        if os.path.exists(self.serverToken):
            statinfo = os.stat(self.serverToken)
            ## if the token is older then 12 hours it is re-downloaded to update the configuration
            oldness = 12*3600
            if (time.time() - statinfo.st_ctime) < oldness: store = False
            else: os.remove(self.serverToken)
        if store:
            command  = "/afs/usr/local/etc/GetToken > %s \n"%self.serverToken
            out, ret = self.ExecuteCommand( command )
            if ret != 0 :
                raise SchedulerError('Error cashing Token ', out, command )
