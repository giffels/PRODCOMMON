#! /usr/bin/env python
"""
_SchedulerRemoteGlidein_
Base class for Remote Glidein scheduler
"""

import os
import stat
import sys
import commands
import subprocess
import re
import shutil
import cStringIO

from socket import getfqdn

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerRemoteglidein(SchedulerInterface) :
    """
    basic class to handle glite jobs through wmproxy API
    """
    def __init__( self, **args ):
        # call super class init method
        super(SchedulerRemoteglidein, self).__init__(**args)
        self.hostname   = getfqdn()
        self.outputDir  = args.get('outputDirectory', None)
        self.jobDir     = args.get('jobDir', None)
        self.shareDir  = args.get('shareDir',None)
        self.remoteDir  = args.get('taskDir',None)
        self.submissionDay  = args.get('submissionDay',None)
        self.sshControlPersist = args.get('sshControlPersist',None)
        self.taskId = ''
        self.userRequirements = ''
        
    def submit( self, obj, requirements='', config ='', service='' ):
        """
        user submission function

        obj is job or list of jobs

        takes as arguments:
        - a finite, dedicated jdl
        - eventually a wms list
        - eventually a config file

        the passed config file or, if not provided, the default one is
        used to extract basic ui configurations and, if not provided, a
        list o candidate wms

        the function returns the grid parent id, the wms of the
        successfully submission and a map associating the jobname to the
        node id. If the submission is not bulk, the parent id is the node
        id of the unique entry of the map

        """

        if not type(obj) == Task :
            raise SchedulerError('Wrong argument type or object type',
                                  str(type(obj)) + ' ' + str(objType))

        self.initializeGsissh(obj)

        self.userRequirements = obj['commonRequirements']

        taskId = obj['name']
        jobCount = 0
            
        submitOptions = ''

        jobRequirements = requirements
        filelist = self.inputFiles(obj['globalSandbox'])

        if filelist:
            fnList=[]
            for fn in filelist.split(','):
                fileName=fn.split('/')[-1]
                fnList.append(fileName)
            shortFilelist= ','.join(fnList)
        jobRequirements += "transfer_input_files = %s\n" % shortFilelist
                
        jdl, sandboxFileList, ce = self.commonJdl(jobRequirements)
        # for some strange reason I need one job to get the executable name
        oneJob=obj.getJobs()[0]
        jdl += 'Executable = %s\n' % (oneJob['executable'])
        jdl += 'log     = condor.log\n'

        jdl += '\n'
        jdl += '+BLTaskID = "' + taskId + '"\n'

        for job in obj.getJobs():
            # Build JDL file
            jdl += self.singleApiJdl(job, jobRequirements)
            jdl += "Queue 1\n"
            jobCount += 1
        # End of loop over jobs to produce JDL

        # Write  JDL

        jdlFileName = self.shareDir + '/' + job['name'] + '.jdl'
        jdlLocalFileName = job['name'] + '.jdl'
        jdlFile = open(jdlFileName, 'w')
        jdlFile.write(jdl)
        jdlFile.close()

        self.logging.info("COPY FILES TO REMOTE HOST")

        # make sure there's a condor work directory on remote host
        command = "%s %s %s " % \
            (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
        command += " mkdir -p %s" % (taskId )
        self.logging.debug("Execute command :\n%s" % command)
        (status, output) = commands.getstatusoutput(command)
        self.logging.debug("Status,output= %s,%s" %
                           (status, output))
        if (status) :
            self.logging.error("Command: %s failed with output=\n%s"%(command,output))

        # copy files to remote host
        filesToCopy = self.inputFiles(obj['globalSandbox']).replace(","," ")
        filesToCopy += " " + jdlFileName
        filesToCopy += " " + self.x509Proxy()

        command = '%s %s %s %s:%s' % \
                  (self.remoteCopyCommand, self.gsisshOptions, \
                       filesToCopy, self.remoteUserHost, taskId)
        self.logging.debug("Execute command :\n%s" % command)
        (status, output) = commands.getstatusoutput(command)
        self.logging.debug("Status,output= %s,%s" %
                           (status, output))
        if (status) :
            self.logging.error("Command: %s failed with output=\n%s"%(command,output))

        # submit

        self.logging.info("SUBMIT TO REMOTE GLIDEIN FRONTEND")

        command = "%s %s %s " % \
            (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
        command += '"cd %s; ' % (taskId)
        command += ' condor_submit %s %s;' % (submitOptions, jdlLocalFileName)
        # in order to look at condor_submit exit code, need a shell dependent addition
        # this is for bash
        # command += ' if [ $? -eq 0 ]; then echo SUBOK; else echo SUBFAIL; fi"'
        # this is for tcsh
        #command += ' if  $? -eq 0  ; then echo SUBOK; else echo SUBFAIL; endif"'
        # try a shell independent shortcut
        command += " echo 'CONDOR_SUBMIT-EXIT-STATUS IS' \$?" + '"'
        self.logging.debug("Execute command :\n%s" % command)
        (status, output) = commands.getstatusoutput(command)
        self.logging.debug("Status,output= %s,%s" %
                           (status, output))

        # Parse output, build numbers

        jobsSubmitted = "CONDOR_SUBMIT-EXIT-STATUS IS 0" in output
        if status and jobsSubmitted:
            self.logging.debug("submission was OK but gsissh reported non-zero exit code, if this happens a lot please do crab -uploadLog and report to crabFeedback")

        if jobsSubmitted :
            ret_map = {}
            jobRegExp = re.compile(
                "\s*(\d+)\s+job\(s\) submitted to cluster\s+(\d+)*")
            for line in output.split('\n'):
                matchObj = jobRegExp.match(line)
                if matchObj:
                    jobsSubmitted = True
                    jobCount = 0
                    for job in obj.getJobs():
                        schedulerID = self.remoteHost + "//"
                        schedulerID += self.submissionDay + "//"
                        schedulerID += matchObj.group(2) + "." + str(jobCount)
                        #condorID = self.remoteHost + "//" \
                        #      + matchObj.group(2) + "." + str(jobCount)
                        ret_map[job['name']] = schedulerID
                        job.runningJob['schedulerId'] = schedulerID
                        jobCount += 1
                    break       # skip lines after condor output

            if jobCount != int(matchObj.group(1)):
                self.logging.error("****** Submitted %s job when %d requested"%\
                                           (matchObj.group(1),jobCount))
                #do a condor_rm of the cluster
                #jobsSubmitted = False

        if not jobsSubmitted:
            job.runningJob.errors.append('Job(s) not submitted: output was\n%s' \
                                         % output )
            self.logging.error("Job not submitted")
            self.logging.error(output)
            #if jobsMaybeSubmitted :
            #    self.logging.error("condor_submit returned error but likely jobs were submitted anyhow. Cleanup may be needed. Do crab -uploadLog and contact support")

        success = self.hostname
        self.logging.debug("Returning %s\n%s\n%s" %
                (ret_map, taskId, success))
        return ret_map, taskId, success


    def findExecHost(self, requirements=''):
        return self.hostname

    def inputFiles(self, globalSandbox):
        """
        Parse out list of input files in sandbox
        """

        filelist = ''
        if globalSandbox is not None:
            for sbFile in globalSandbox.split(','):
                if sbFile == '' :
                    continue
                filename = os.path.abspath(sbFile)
                filename.strip()
                filelist += filename + ','
        return filelist[:-1] # Strip off last ","


    def commonJdl(self, requirements=''):
        """
        Bulk mode, common things for all jobs
        """
        jdl  = self.specificBulkJdl(requirements='')
        jdl += 'stream_output = false\n'
        jdl += 'stream_error  = false\n'
        jdl += 'notification  = never\n'
        jdl += 'should_transfer_files   = YES\n'
        jdl += 'when_to_transfer_output = ON_EXIT\n'
        jdl += 'copy_to_spool           = false\n'

        # Things in the requirements/jobType field
        jdlLines = requirements.split(';')
        ce = None
        for line in jdlLines:
            [key, value] = line.split('=', 1)
            if key.strip() == "schedulerList":
                ceList = value.split(',')
                ce = ceList[0].strip()
                jdl += "grid_resource = gt2 " + ce + '\n'
            else:
                jdl += line.strip() + '\n'
        filelist = ''
        return jdl, filelist, ce
    
    def specificBulkJdl(self, requirements=''):
        # This is taken from SchedulerGlidein with minor changes for proxy handling

        """
        build a job jdl
        """

        jdl  = 'Universe  = vanilla\n'

        # Glidein parameters
        jdl += 'Environment = ' \
                'CONDOR_ID=$(Cluster).$(Process);' \
               'JobRunCount=$$([ ifThenElse(' \
                'isUndefined(JobRunCount),0,JobRunCount) ]);' \
               'GlideinMemory=$$([ ifThenElse(' \
                'isUndefined(ImageSize_RAW),0,ImageSize_RAW) ]);' \
               'Glidein_MonitorID=$$([ ifThenElse(' \
                'isUndefined(Glidein_MonitorID),0,Glidein_MonitorID) ]) \n'
        jdl += 'since=(CurrentTime-EnteredCurrentStatus)\n'
        # remove Running jobs after MaxWallTime
        jdl += 'Periodic_Remove = ((JobStatus == 2) && ' \
               '(((CurrentTime - JobCurrentStartDate) > ' \
                '(MaxWallTimeMins*60)) =?= True)) || '
        # remove 5-Held and 1-Idle jobs after 8 days
        jdl += '(JobStatus==5 && $(since)>691200) || ' \
               '(JobStatus==1 && $(since)>691200)\n'

        if self.userRequirements:
            jdl += 'requirements = %s\n' % self.userRequirements

        x509 = self.x509Proxy()
        if x509:
            proxyName=x509.split('/')[-1]
            jdl += 'x509userproxy = %s\n' % proxyName
            
        return jdl

    def singleApiJdl(self, job, requirements=''):
        """
        build a job jdl
        """

        jdl  = ''
        jobId = int(job['jobId'])
        # Make arguments condor friendly (space delimited w/o backslashes)
        jobArgs = job['arguments']
        # Server args already correct
        jobArgs = jobArgs.replace(',',' ')
        jobArgs = jobArgs.replace('\\ ',',')
        jobArgs = jobArgs.replace('\\','')
        jobArgs = jobArgs.replace('"','')

        jdl += 'Arguments  = %s\n' % jobArgs
        if job['standardInput'] != '':
            jdl += 'input = %s\n' % job['standardInput']
        jdl += 'output  = %s\n' % job['standardOutput']
        jdl += 'error   = %s\n' % job['standardError']
        jdl += 'transfer_output_remaps   = "%s=/dev/null; %s=/dev/null"\n' % (job['standardError'], job['standardOutput'])

        # HACK: Figure out where the request for .BrokerInfo comes from
        outputFiles = []
        for fileName in job['outputFiles']:
            if not fileName.endswith('BrokerInfo'):
                outputFiles.append(fileName)
        if outputFiles:
            jdl += 'transfer_output_files   = ' + ','.join(outputFiles) + '\n'


        return jdl


    def query(self, obj, service='', objType='node'):
        """
        query status of jobs
        """

        from xml.sax import make_parser
        from CondorHandler import CondorHandler
        from xml.sax.handler import feature_external_ges

        # FUTURE:
        #  Use condor_history -attributes to limit the XML size. Faster on both ends
        # Convert Condor integer status to BossLite Status codes
        # Condor status is e.g. from http://pages.cs.wisc.edu/~adesmet/status.html#condor-jobstatus
        # 0	Unexpanded 	U
        # 1	Idle 	        I
        # 2	Running 	R
        # 3	Removed 	X
        # 4	Completed 	C
        # 5	Held 	        H
        # 6	Submission_err 	E

        statusCodes = {'0':'RE', '1':'S', '2':'R',
                       '3':'K',  '4':'SD', '5':'A'}
        textStatusCodes = {
                '0':'Ready',
                '1':'Submitted',
                '2':'Running',
                '3':'Cancelled',
                '4':'Done',
                '5':'Aborted'
        }


        if not type(obj) == Task:
            raise SchedulerError('Wrong argument type or object type',
                                  str(type(obj)) + ' ' + str(objType))

        taskId = obj['name']
        
        jobIds = {}
        bossStatus = {}
        schdId = {}

        somethingDone = False # note if some jobs became Done in this round

        for job in obj.jobs:
            if not self.valid(job.runningJob):
                continue
            
            # skip if the Job was created but never submitted
            if job.runningJob['status'] == 'C' :
                continue

            # skip if the Job is already Done, nothing more to ask glidein
            if job.runningJob['statusScheduler'] == 'Done' :
                continue
            
            schedulerId = job.runningJob['schedulerId']

            # Jobs are done if condor_q/history does not list them
            # queries to condor schedd's will only return cluster.job
            # so needs to cross link the two via the schdId[condoId] map
            bossStatus[schedulerId] = {'status':'SD', 'statusScheduler':'Done'}
            schedd = schedulerId.split('//')[0]
            submissionDay = schedulerId.split('//')[1]
            jobNum = schedulerId.split('//')[2]
            condorId = schedd + '//' + jobNum
            schdId[condorId] = schedulerId

            # Fill dictionary of schedd and job #'s to check
            if schedd in jobIds.keys():
                jobIds[schedd].append(jobNum)
            else :
                jobIds[schedd] = [jobNum]

        if len(jobIds.keys()) > 0 :
            # there is something to check on remote condor host
            self.initializeGsissh(obj)

        for schedd in jobIds.keys() :
            if not schedd == self.remoteHost:
                self.logging.info("ERROR: found jobs for schedd %s in a task targetted for submission host %s" % (schedd,self.remoteHost))
                raise Exception("Mixing schedd's in same task is not supported")
            
            # to begin with, push a fresh proxy to the remote host
            command = '%s %s %s %s:%s' % \
                      (self.remoteCopyCommand, self.gsisshOptions, \
                           self.x509Proxy(), self.remoteUserHost, taskId)
            self.logging.debug("Execute command :\n%s" % command)
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Status,output= %s,%s" %
                    (status, output))
            if (status) :
                self.logging.error("Failed to renew proxy on remote submission host")
                self.logging.error("Command: %s failed with output=\n%s"%(command,output))

            command = "%s %s %s " % \
                (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
            command += ' "condor_history -userlog %s/condor.log' % taskId
            command += ' -xml"'

            self.logging.debug("Execute command :\n%s" % command)

            pObj = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, close_fds=True)
            (inputFile, outputFp, errorFp) = (pObj.stdin, pObj.stdout, pObj.stderr)
            try:
                outputFile = cStringIO.StringIO(outputFp.read()) # >7.3 vers.
            except:
                raise SchedulerError('Problem reading output of command', command)

            # If the command succeeded, close returns None
            # Otherwise, close returns the exit code
            if outputFp.close():
                raise SchedulerError("condor_history command or cache file failed.")
            else:   # close stderr from command ignoring errors
                try:
                    errorFp.close()
                except:
                    pass

            handler = CondorHandler('GlobalJobId',
                       ['JobStatus', 'GridJobId','ProcId','ClusterId',
                        'JOB_Gatekeeper', 'MATCH_GLIDEIN_Gatekeeper','GlobalJobId'])

            parser = make_parser()
            try:
                parser.setContentHandler(handler)
                parser.setFeature(feature_external_ges, False)
                parser.parse(outputFile)
            except:
                self.logging.info("Unexpected exception: %s" % sys.exc_info()[0])
                self.logging.debug("Error parsing condor_history output:\n%s" % outputFile.getvalue())
                raise SchedulerError('Problem parsing output of condor_history command', command)

            jobDicts = handler.getJobInfo()


            for globalJobId in jobDicts.keys():
                clusterId = jobDicts[globalJobId].get('ClusterId', None)
                procId    = jobDicts[globalJobId].get('ProcId',    None)
                jobId = str(clusterId) + '.' + str(procId)
                condorId = schedd + '//' + jobId
                jobStatus = jobDicts[globalJobId].get('JobStatus', None)

                # Host can be either in Job_Gatekeeper or MATCH_GLIDEIN_Gatekeeper
                execHost = None
                glideinHost = None
                jobGkpr = jobDicts[globalJobId].get('JOB_Gatekeeper', None)
                matGkpr = jobDicts[globalJobId].get('MATCH_GLIDEIN_Gatekeeper', None)
                if jobGkpr and not "Unknown" in jobGkpr:
                    glideinHost = jobGkpr
                else:
                    if matGkpr:
                        glideinHost = matGkpr

                if glideinHost:
                    execHost = glideinHost
                    # strip possible leading https's and leading/trailing extra words
                    for token in glideinHost.replace("https://","").split(" ") :
                        if token.find("/") != -1 :
                            execHost = token

                # Don't mess with jobs we're not interested in,
                # put what we found into BossLite statusRecord
                
                if schdId.has_key(condorId):
                #if bossStatus.has_key(schedd+'//'+jobId):
                    schedulerId = schdId[condorId]
                    statusRecord = {}
                    statusRecord['status']          = statusCodes.get(jobStatus, 'UN')
                    statusRecord['statusScheduler'] = textStatusCodes.get(jobStatus, 'Undefined')
                    statusRecord['statusReason']    = ''
                    statusRecord['service']         = service
                    if execHost:
                        statusRecord['destination'] = execHost
                    if statusRecord['status'] == 'SD' :
                        somethingDone = True

                    bossStatus[schedulerId] = statusRecord

        if somethingDone :
            # get ExitCodes from fjrs"
            command = "%s %s %s " % \
            (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
            command += '"cd %s; ' % (taskId)
            # need to put single and double quotes and tab (\t) in
            # shell command for gsissh. So get horrible escaping here
            # be very careful with changes
            command += "egrep -H WrapperExitCode\|ExeExitCode crab_fjr_*xml"
            command += "|tr '_.\\t\\\"' ' '"   # change all delim to blank for awk
            command += "|awk '{print \\$3\\\" \\\"\\$7\\\" \\\"\\$9}'\""
            self.logging.debug("Execute command :\n%s" % command)
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Status,output= %s\n%s" %
                               (status, output))

            ExeCodes={}
            WrapperCodes={}
            for line in output.split('\n'):
                jid,code,kind=line.split(' ')
                if kind == 'ExeExitCode' :
                    ExeCodes[int(jid)]=code
                if kind == 'WrapperExitCode' :
                    WrapperCodes[int(jid)]=code
        
        for job in obj.jobs:      # loop on crab job id's
            jid=job.runningJob['jobId']
            schedulerId = job.runningJob['schedulerId']
            if bossStatus.has_key(schedulerId):  # there's an update from condor
                for key, value in bossStatus[schedulerId].items():
                    job.runningJob[key] = value
                # if this a newly terminated job get the exit code w/o waiting for crab -get
                if somethingDone :
                    if ExeCodes.has_key(jid):
                        job.runningJob['applicationReturnCode']=ExeCodes[jid]
                        if WrapperCodes.has_key(jid):
                            job.runningJob['wrapperReturnCode']=WrapperCodes[jid]

        return


    def kill( self, obj ):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        self.initializeGsissh(obj)

        for job in obj.jobs:
            if not self.valid( job.runningJob ):
                continue
            schedulerId = str(job.runningJob['schedulerId']).strip()
            jobId  = schedulerId.split('//')[-1]

            command = '%s %s %s ' \
                % (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
            command += ' "condor_rm  %s"' % (jobId)

            self.logging.debug("Execute command :\n%s" % command)
            try:
                retcode = subprocess.call(command, shell=True)
            except OSError, ex:
                raise SchedulerError('condor_rm failed', ex)
        return


    def getOutput( self, obj, outdir='' ):
        """
        Retrieve (move) job output from cache directory to outdir
        """

        self.initializeGsissh(obj)

        if type(obj) == RunningJob: # The object passed is a RunningJob
            raise SchedulerError('Operation not possible',
                  'Condor cannot retrieve files when passed RunningJob')
        elif type(obj) == Job: # The object passed is a Job

            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # retrieve output
            self.getCondorOutput(obj, outdir)

        # the object passed is a Task
        elif type(obj) == Task :

            taskId = obj['name']
            self.taskId = taskId
            if outdir == '':
                outdir = obj['outputDirectory']

            for job in obj.jobs:
                if self.valid( job.runningJob ):
                    self.getCondorOutput(job, outdir)

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    def getCondorOutput(self, job, outdir):
        """
        Move the files for Condor from remote directory to
        final resting place
        """

        fileList = job['outputFiles']
        for fileName in fileList:
            targetFile = outdir + '/' + fileName
            subCounter = 0
            while os.path.exists( targetFile ):
            # if file exists already make an attempt to save old copy in Submission_x subdir, but if that fails plow on and overwrite with the new one
                subCounter = subCounter + 1
                try:
                    temporaryDir = "%s/Submission_%s" % (outdir, subCounter)
                    try:
                         os.mkdir( temporaryDir )
                    except : # ignore problems
                        pass # Double nest the try blocks to keep the mkdir
                             # from incrementing the subCounter
                    shutil.move( targetFile, temporaryDir )
                except :
                    pass #ignore problems

            try:
                command = '%s %s %s:%s/' % \
                          (self.remoteCopyCommand, self.gsisshOptions, \
                               self.remoteUserHost, self.taskId)
                command += fileName + " " + outdir
                self.logging.info("RETRIEVE FILE %s for job #%d" % (fileName, job['jobId']))
                self.logging.debug("Execute command :\n%s" % command)
                (status, output) = commands.getstatusoutput(command)
                self.logging.debug("Status,output= %s,%s" %
                    (status, output))
                if (status) :
                    # did copy really fail ?
                    if not os.access(outdir+'/'+fileName,os.F_OK) :
                        msg="Could not retrieve file %s." % fileName
                        self.logging.error( msg )
                        msg += " Command: %s failed with status,output=\n%d\n%s"%(command,status,output)
                        job.runningJob.errors.append( msg )
            except :
                msg="Could not retrieve file %s." % fileName
                self.logging.error( msg )
                msg += " Unexpected exception: %s" % sys.exc_info()[0]
                job.runningJob.errors.append( msg )


    def postMortem( self, obj, schedulerId, outfile, service):
        """
        Get detailed postMortem job info
        """

        if not type(obj) == Task:
            raise SchedulerError('Wrong argument type or object type',
                                  str(type(obj)) + ' ' + str(objType))

        if not outfile:
            raise SchedulerError('Empty filename',
                                 'postMortem called with empty logfile name')

        taskId = obj['name']
        condorId = schedulerId.split('//')[-1]
        header = '========= LOGGING INFO FOR %s =========\n' % schedulerId
        horsep = '\n'+80*'='+'\n'
        sep1 =   '\n========= OUTPUT OF : Condor_history -l %s =========\n' % condorId
        sep2 =   '\n========= OUTPUT OF : Condor_q -l  %s =========\n' % condorId
        
        self.initializeGsissh(obj)

        fp=open(outfile,'w')
        fp.write(header)
        fp.write(horsep)
        fp.write(sep1)
        
        command = '%s %s %s ' \
                  % (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
        command += ' "condor_history -userlog %s/condor.log -l %s"' % \
                   (taskId, condorId)
        (status, output) = commands.getstatusoutput(command)

        fp.write(output)
        fp.write(horsep)

        # the following condor_q only makes sense if job status
        # is 1(Idle), 2(Run) or 5(Held) but may cost little to do always

        fp.write(sep2)
        command = '%s %s %s ' \
                  % (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
        command += ' "condor_q -l %s"' % condorId
        (status, output) = commands.getstatusoutput(command)
        
        fp.write(output)
        fp.write('\n')
        fp.write(horsep)
        fp.close()

        return

    def jobDescription(self, obj, requirements='', config='', service = ''):
        """
        retrieve scheduler specific job description
        """

        return "Check jdl files in " + self.shareDir + " after submit\n"


    def x509Proxy(self):
        """
        Return the name of the X509 proxy file (must exist)
        """
        x509 = None
        x509tmp = '/tmp/x509up_u' + str(os.getuid())
        if 'X509_USER_PROXY' in os.environ:
            x509 = os.environ['X509_USER_PROXY']
        elif os.path.isfile(x509tmp):
            x509 = x509tmp
        return x509

    def initializeGsissh(self,obj):
        import time
        from zlib import adler32
        import shlex
        
        if obj['serverName'] :
            # cast to string to avoid issues with unicode later in shlex :-(
            self.remoteUserHostCommand = str(obj['serverName'])
            self.logging.info("contacting remote host %s" % self.remoteUserHostCommand)
        else:
            raise SchedulerError("ERROR!!!! no serverName in task ",obj)
        
        if ':' in self.remoteUserHostCommand:
            self.remoteCommand = self.remoteUserHostCommand.split(':')[0]
            if self.remoteCommand == 'ssh':
                self.remoteCopyCommand = 'scp'
            else:
                raise SchedulerError('Fatal','no copy command defined')
            self.remoteUserHost = self.remoteUserHostCommand.split(':')[1]
        else:
            self.remoteCommand = 'gsissh'
            self.remoteCopyCommand = 'gsiscp'
            self.remoteUserHost = self.remoteUserHostCommand
        self.remoteHost = self.remoteUserHostCommand.split('@')[-1]
        
        # uniquely identify the ssh link for this gsissh connection
        # to be reusable by subsequent crab command with same credentials,
        # so use voms id + fqan  and remote host name
        # ControlPath link can not be in $HOME/.ssh since e.g. does
        # not work on AFS, I am sticking with /tmp
        # need to make sure this file is owned by user and protected
        # to avoid someone stealing the control socket
        # at least on lxplus /tmp/<username> appear to be there already
        # and have proper protection, so pick that as start point
        # anyhow verify /tmp/<username> as well

        tmpDir="/tmp/%s" % os.environ['LOGNAME']
        sshLinkDir="/tmp/%s/.ssh/" % os.environ['LOGNAME']
        command = "voms-proxy-info -id"
        vomsId = commands.getoutput(command)
        command = "voms-proxy-info -fqan | head -1"
        vomsId += commands.getoutput(command)
        sshLink = sshLinkDir + "ssh-link-%s-%s" % \
            (adler32(vomsId), self.remoteHost)
        self.gsisshOptions = "-o ControlMaster=auto"
        self.gsisshOptions += " -o ControlPath=%s" % sshLink

        # also do not rely on local user's cofig
        self.gsisshOptions += " -o BatchMode=yes"
        self.gsisshOptions += " -o StrictHostKeyChecking=no"
        self.gsisshOptions += " -o ForwardX11=no"


        # ControlPersist is not supported by current gsissh
        # therefore fork and renew a 20min long
        # ssh connection in background to speed up
        # successive gsissh/gsiscp commands
        # try to make sure there are always 10 more minutes

        sshLinkOK = False
        # make sure we have a properly safe directory where to put the CP link

        if not os.access(tmpDir, os.F_OK) : 
            try:
                os.makedirs(tmpDir, 0700)   # mode 700 octal only owner can access
            except:
                self.logging.error("CAN'T CREATE SAFE DIRECTORY %s. CANNOT GO ON"%tmpDir)
                raise SchedulerError('Fatal','SECURITY COMPROMISED')
                
        tmpDirMine = os.stat(tmpDir).st_uid == os.getuid() # I own
        if not tmpDirMine :
            list = subprocess.Popen(['ls', '-ld', tmpDir], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
            msg  = "DIRECTORY %s EXISTS BUT NOT OWNED. CANNOT GO ON\n" % tmpDir
            msg += list
            msg += "\nFIND OUT WHAT HAPPENED, REMOVE IT, RETRY"
            self.logging.error(msg)
            raise SchedulerError('Fatal','SECURITY COMPROMISED')

        if not os.access(sshLinkDir, os.F_OK) : 
            try:
                os.makedirs(sshLinkDir, 0700)   # mode 700 octal only owner can access
            except:
                self.logging.error("CAN'T CREATE SAFE DIRECTORY %s. CANNOT GO ON"%sshLinkDir)
                raise SchedulerError('Fatal','SECURITY COMPROMISED')
                
        sshDirMine = os.stat(sshLinkDir).st_uid == os.getuid() and  \
               stat.S_IMODE(os.stat(sshLinkDir).st_mode) ==  0700     # I own and permission is 700 octal

        if not sshDirMine :
            list = subprocess.Popen(['ls', '-ld', sshLinkDir], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
            msg  = "DIRECTORY %s EXISTS BUT NOT OWNED OR BAD PROTECTION. CANNOT GO ON\n" % sshLinkDir
            msg += list
            msg += "\nFIND OUT WHAT HAPPENED, REMOVE IT, RETRY"
            self.logging.error(msg)
            raise SchedulerError('Fatal','SECURITY COMPROMISED')
            

        # so far so good sshLinkDir is OK
        if os.access(sshLink, os.F_OK) :  # a CtrlPath link is there already
            sshLinkMine = os.stat(sshLink).st_uid == os.getuid() and \
                          stat.S_IMODE(os.stat(sshLink).st_mode) == 0600  # I own and permission in 600 octal
            if not sshLinkMine :
                #scream loud
                self.logging.error("SECURITY HAZARD: ssh control link %s exists but not owned !"%sshLink)
                try:
                    os.unlink(sshLink)
                except:
                    self.logging.error("CAN'T REMOVE SPURIOUS control link %s. CANNOT GO ON"%ssLink)
                    raise SchedulerError('Fatal','SECURITY COMPROMISED')
            else :
                sshLinkOK = True

        else :
            # no problem, will be created as needed
            sshLinkOK = False

        # which kind of socket is required ?
        if self.sshControlPersist.lower() == "no" :
            # since nothing is requested, pretend whatever is there is OK
            sshLinkOK = True
        if self.sshControlPersist.lower() == "yes" :
            # need to create a permanent socket, no harm in doing twice
            sshLinkOK = False
            command = "%s  -N -n %s %s /bin/true" % \
                (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
        if self.sshControlPersist.isdigit() :
            # interprete as persist time in seconds
            persistTime=int(self.sshControlPersist)
            if sshLinkOK :
                linkTime=(time.time() - os.stat(sshLink).st_ctime)
                sshLinkOK = (persistTime - linkTime) > persistTime/2

            command = "%s  %s %s " % \
                      (self.remoteCommand, self.gsisshOptions, self.remoteUserHost)
            command += ' "sleep %s"' % persistTime

        #self.logging.info("SB sshLinkOK after all checks = %s" % sshLinkOK)

        if not sshLinkOK :
            # CP link is either missing or expiring in less then half
            # the desired persist time, create a gsissh connection
            # to keep CP link alive

            #self.logging.debug("Execute command :\n%s" % command)
            self.logging.info("Execute command :\n%s" % command)
            bkgGsissh = subprocess.Popen(shlex.split(command))

            # make sure the ControlPath link is there before going on
            # to avoid races with later gsi* commands
            while not os.access(sshLink, os.F_OK) :
                self.logging.info("Establishing %s ControlPath. Wait 2 sec ..."%self.remoteCommand)
                time.sleep(2)
            # update time stamp of ssh CP link to signal that it was renewed
            os.utime(sshLink,None)

        return
