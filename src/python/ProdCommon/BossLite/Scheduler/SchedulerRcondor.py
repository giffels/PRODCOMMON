#! /usr/bin/env python
"""
_SchedulerRcondor_
Base class for Remote Condor scheduler
"""

import os
import sys
import commands
import subprocess
import shlex
import re
import shutil
import cStringIO

from socket import getfqdn

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerRcondor(SchedulerInterface) :
    """
    basic class to handle glite jobs through wmproxy API
    """
    def __init__( self, **args ):
        # call super class init method
        super(SchedulerRcondor, self).__init__(**args)
        os.environ['_CONDOR_GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE'] = '20'
        self.hostname   = getfqdn()
        self.outputDir  = args.get('outputDirectory', None)
        self.jobDir     = args.get('jobDir', None)
        self.shareDir  = args.get('shareDir',None)
        self.remoteDir  = args.get('taskDir',None)
        self.taskId = ''
        self.useGlexec  = args.get('useGlexec', False)
        self.glexec     = args.get('glexec', None)
        self.renewProxy    = args.get('renewProxy', None)
        self.glexecWrapper = args.get('glexecWrapper', None)
        self.condorQCacheDir     = args.get('CondorQCacheDir', None)
        self.userRequirements = ''
        self.rcondorHost = args.get('rcondorHost', None)
        self.rcondorUser = os.getenv('RCONDOR_USER')
        if not self.rcondorUser :
            self.logging.info("$RCONDOR_USER not defined, try to find out via uberftp ...")
            command="uberftp $RCONDOR_HOST pwd|grep User|awk '{print $3}'"
            (status, output) = commands.getstatusoutput(command)
            if status == 0:
                self.rcondorUser = output
                self.logging.info("rcondorUser set to %s" % self.rcondorUser)
        if self.rcondorUser==None:
            raise Exception('FATAL ERROR: env.var RCONDOR_USER not defined')
        
        self.gsisshOptions = "-o ControlMaster=auto -o ControlPath=/tmp/link-%r@%h.%p"
        
        # ControlPersist is not supported by current gssapi
        # so will fork a 60sec ssh connection in background to speed up
        # successive gsissh/gsiscp commands
        # make sure the ControlPath link is there before going on
        # in order to avoid races with later gsi* commands
        command = "gsissh  %s %s@%s " % (self.gsisshOptions, self.rcondorUser, self.rcondorHost)
        command += ' -n "sleep 60" 2>&1 > /dev/null'
        bkgGsissh = subprocess.Popen(shlex.split(command))
        sshLink="/tmp/link-%s@%s.22" % (self.rcondorUser, self.rcondorHost)
        while not os.access(sshLink, os.F_OK) :
          self.logging.info("wait 2 sec for gsissh ControlPath")
          subprocess.call(shlex.split("sleep 2"))

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

        self.userRequirements = obj['commonRequirements']

        submissionHost = self.rcondorHost

        taskId = ''
        ret_map = {}

        jobRegExp = re.compile(
                "\s*(\d+)\s+job\(s\) submitted to cluster\s+(\d+)*")
        if type(obj) == RunningJob or type(obj) == Job :
            raise NotImplementedError
        elif type(obj) == Task :
            taskId = obj['name']
            jobCount = 0
            jdl = ''
            
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
            command = "gsissh %s %s@%s " % (self.gsisshOptions, self.rcondorUser, submissionHost)
            command += " mkdir -p %s" % (taskId )
            self.logging.debug("Execute command :\n%s" % command)
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Status,output= %s,%s" %
                    (status, output))


            # copy files to remote host
            filesToCopy = self.inputFiles(obj['globalSandbox']).replace(","," ")
            filesToCopy += " " + jdlFileName
            filesToCopy += " " + self.x509Proxy()

            command = 'gsiscp %s %s %s@%s:%s' % \
                      (self.gsisshOptions, filesToCopy, self.rcondorUser, submissionHost, taskId)
            self.logging.debug("Execute command :\n%s" % command)
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Status,output= %s,%s" %
                    (status, output))


            # submit

            self.logging.info("SUBMIT TO REMOTE CONDOR ")
            command = "gsissh %s %s@%s " % (self.gsisshOptions, self.rcondorUser, submissionHost)
            #command +== '"cd %s; ' % (taskId)
            command += ' "cd %s; condor_submit %s %s"' % (taskId, submitOptions, jdlLocalFileName)
            self.logging.debug("Execute command :\n%s" % command)
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Status,output= %s,%s" %
                    (status, output))

            # Parse output, build numbers
            jobsSubmitted = False
            if not status:
                for line in output.split('\n'):
                    matchObj = jobRegExp.match(line)
                    if matchObj:
                        jobsSubmitted = True
                        jobCount = 0
                        for job in obj.getJobs():
                            condorID = submissionHost + "//" \
                               + matchObj.group(2) + "." + str(jobCount)
                            ret_map[job['name']] = condorID
                            job.runningJob['schedulerId'] = condorID
                            jobCount += 1
            if not jobsSubmitted:
                job.runningJob.errors.append('Job not submitted:\n%s' \
                                                % output )
                self.logging.error("Job not submitted:")
                self.logging.error(output)

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
        jdl += 'Periodic_Remove = (((JobStatus == 2) && ' \
               '((CurrentTime - JobCurrentStartDate) > ' \
                '(MaxWallTimeMins*60))) =?= True) || '
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
        if not self.useGlexec:
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

        jobIds = {}
        bossIds = {}

        # FUTURE:
        #  Use condor_q -attributes to limit the XML size. Faster on both ends
        # Convert Condor integer status to BossLite Status codes
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

        if type(obj) == Task:
            taskId = obj['name']

            for job in obj.jobs:
                if not self.valid(job.runningJob):
                    continue

                schedulerId = job.runningJob['schedulerId']

                # fix: skip if the Job was created but never submitted
                if job.runningJob['status'] == 'C' :
                    continue

                # Jobs are done if condor_q does not list them
                bossIds[schedulerId] = {'status':'SD', 'statusScheduler':'Done'}
                schedd = schedulerId.split('//')[0]
                jobNum = schedulerId.split('//')[1]

                # Fill dictionary of schedd and job #'s to check
                if schedd in jobIds.keys():
                    jobIds[schedd].append(jobNum)
                else :
                    jobIds[schedd] = [jobNum]
        else:
            raise SchedulerError('Wrong argument type or object type',
                                  str(type(obj)) + ' ' + str(objType))

        for schedd in jobIds.keys() :
            submissionHost = schedd
            
            # to begin with, push a fresh proxy to the remote host
            
            command = 'gsiscp %s %s %s@%s:%s' % \
                      (self.gsisshOptions, self.x509Proxy(), self.rcondorUser, submissionHost, taskId)
            self.logging.debug("Execute command :\n%s" % command)
            (status, output) = commands.getstatusoutput(command)
            self.logging.debug("Status,output= %s,%s" %
                    (status, output))

            command = "gsissh %s %s@%s " % (self.gsisshOptions, self.rcondorUser, submissionHost)
            command += ' "condor_q -userlog %s/condor.log' % taskId
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
                raise SchedulerError("condor_q command or cache file failed.")
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
                self.logging.debug("Error parsing condor_q output:\n%s" % outputFile.getvalue())
                raise SchedulerError('Problem parsing output of condor_q command', command)

            jobDicts = handler.getJobInfo()


            for globalJobId in jobDicts.keys():
                clusterId = jobDicts[globalJobId].get('ClusterId', None)
                procId    = jobDicts[globalJobId].get('ProcId',    None)
                jobId = str(clusterId) + '.' + str(procId)
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
                if bossIds.has_key(schedd+'//'+jobId):
                    statusRecord = {}
                    statusRecord['status']          = statusCodes.get(jobStatus, 'UN')
                    statusRecord['statusScheduler'] = textStatusCodes.get(jobStatus, 'Undefined')
                    statusRecord['statusReason']    = ''
                    statusRecord['service']         = service
                    if execHost:
                        statusRecord['destination'] = execHost

                    bossIds[schedd + '//' + jobId] = statusRecord


        for job in obj.jobs:
            schedulerId = job.runningJob['schedulerId']
            if bossIds.has_key(schedulerId):
                for key, value in bossIds[schedulerId].items():
                    job.runningJob[key] = value

        return


    def kill( self, obj ):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        for job in obj.jobs:
            if not self.valid( job.runningJob ):
                continue
            schedulerId = str(job.runningJob['schedulerId']).strip()
            submissionHost, jobId  = schedulerId.split('//')

            command = 'gsissh %s %s@%s ' (self.gsisshOptions, self.rcondorUser, submissionHost)
            command += ' "condor_rm -name %s %s"' % (submissionHost, jobId)

            self.logging.info("Execute command :\n%s" % command)
            try:
                retcode = call(command, shell=True)
            except OSError, ex:
                raise SchedulerError('condor_rm failed', ex)
        return


    def getOutput( self, obj, outdir='' ):
        """
        Retrieve (move) job output from cache directory to outdir
        User files from CondorG appear asynchronously in the cache directory
        """

        if type(obj) == RunningJob: # The object passed is a RunningJob
            raise SchedulerError('Operation not possible',
                  'CondorG cannot retrieve files when passed RunningJob')
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

        submissionHost=job.runningJob['schedulerId'].split('//')[0]

        fileList = job['outputFiles']
        for fileName in fileList:
            targetFile = outdir + '/' + fileName
            subCounter = 0
            while os.path.exists( targetFile ):
                subCounter = subCounter + 1
                try:
                    temporaryDir = "%s/Submission_%s" % (outdir, subCounter)
                    try:
                        os.mkdir( temporaryDir )
                    except IOError:
                        pass # Double nest the try blocks to keep the mkdir
                             # from incrementing the subCounter
                    except OSError:
                        pass
                    shutil.move( targetFile, temporaryDir )
                except IOError:
                    pass #ignore problems
                
            try:
                command = 'gsiscp %s %s@%s:%s/' % \
                          (self.gsisshOptions, self.rcondorUser, submissionHost, self.taskId)
                command += fileName + " " + outdir
                self.logging.info("RETRIEVE FILE %s for job #%d" % (fileName, job['jobId']))
                self.logging.debug("Execute command :\n%s" % command)
                (status, output) = commands.getstatusoutput(command)
                self.logging.debug("Status,output= %s,%s" %
                    (status, output))
            except IOError:
                self.logging.error( "Could not move file %s" % fileName)



    def postMortem( self, schedulerId, outfile, service):
        """
        Get detailed postMortem job info
        """

        raise SchedulerError('NotImplemented','postMortem not implemented for this scheduler')

        #if not outfile:
        #    raise SchedulerError('Empty filename',
        #                         'postMortem called with empty logfile name')

        #submissionHost, jobId = schedulerId.split('//')
        #command = "condor_history -l -name  %s %s > %s" % (submissionHost, jobId, outfile)
        #return self.ExecuteCommand(command)

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
