#!/usr/bin/env python
"""
_SchedulerCondorCommon_
Base class for CondorG and GlideIn schedulers
"""

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.28.2.13 2008/09/22 16:47:22 ewv Exp $"
__version__ = "$Revision: 1.28.2.13 $"

# For earlier history, see SchedulerCondorGAPI.py

import sys
import os
import popen2
import re
import shutil
import cStringIO

from socket import getfqdn

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerCondorCommon(SchedulerInterface) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, **args ):
    # call super class init method
    self.debugLog = open('/tmp/CondorGServer.log','a')

    super(SchedulerCondorCommon, self).__init__(**args)
    self.hostname   = getfqdn()
    self.condorTemp = args.get('tmpDir', None)
    self.outputDir  = args.get('outputDirectory', None)
    self.useGlexec  = args.get('useGlexec', False)
    self.batchSize  = 20 # Number of jobs to submit per site before changing CEs
    self.glexecWrapper = 'glexecWrapper.sh'


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

    # Make directory for Condor returned files

    seDir = "/".join((obj['globalSandbox'].split(',')[0]).split('/')[:-1])
    exeName = obj['scriptName'].split('/')[-1]
    exeName = 'CMSSW.sh' # HACK since no trailing slash on hostname
    if os.path.isdir(self.condorTemp):
      pass
    else:
      os.mkdir(self.condorTemp)

    if self.useGlexec:
        # Write the submitter script
        stdout, stdin  = popen2.popen2('which condor_submit')
        condorSubmit = stdout.read().strip()

        cacheDir = os.getcwd()
        os.chdir(self.condorTemp)
        wrapper = open(self.glexecWrapper, 'w')
        wrapper.writelines([
            '#! /bin/sh\n',
            'cd %s\n' % seDir,
            'export X509_USER_PROXY=$PWD/userProxy\n',
            '%s $1\n' % condorSubmit,
            ])
        wrapper.close()
        os.chmod(self.glexecWrapper, 0755)
        os.chdir(cacheDir)

        # Set up the environment
        os.environ['GLEXEC_CLIENT_CERT']  = obj['user_proxy']
        os.environ['GLEXEC_SOURCE_PROXY'] = obj['user_proxy']
        os.environ['GLEXEC_TARGET_PROXY'] = seDir + '/userProxy'
    # Get list of schedd's
    scheddList = None
    nSchedd    = 0
    if 'CMS_SCHEDD_LIST' in os.environ:
      scheddList = os.environ['CMS_SCHEDD_LIST'].split(',')
      nSchedd    = len(scheddList)

    configfile = config

    taskId = ''
    ret_map = {}

    jobRegExp = re.compile("\s*(\d+)\s+job\(s\) submitted to cluster\s+(\d+)*")

    if type(obj) == RunningJob or type(obj) == Job :
      raise NotImplementedError
    elif type(obj) == Task :
      taskId = obj['name']
      jobCount = 0
      for job in obj.getJobs():
        submitOptions = ''
        if scheddList:
          schedd = scheddList[jobCount%nSchedd]
          submitOptions += '-name %s ' % schedd
        if requirements:
            jobRequirements = requirements
        else:
            jobRequirements = obj['jobType']
        execHost = self.findExecHost(jobRequirements)
        filelist = self.inputFiles(obj['globalSandbox'])
        if filelist:
          jobRequirements += "transfer_input_files = " + filelist + '\n'
        # Build JDL file

        jdl, sandboxFileList, ce = self.decode(job, jobRequirements)
 
        jdl += 'Executable = %s/%s\n' % (seDir, exeName)
        jdl += '+BLTaskID = "' + taskId + '"\n'
        # If query were to take a task could then do something like
        # condor_q -constraint 'BLTaskID == "[taskId]"' to retrieve just those jobs
        jdl += "Queue 1\n"
 
        # Write and submit JDL
        jdlFileName = job['name']+'.jdl'
        cacheDir = os.getcwd()
        os.chdir(self.condorTemp)
        jdlFile = open(jdlFileName, 'w')
        jdlFile.write(jdl)
        jdlFile.close()
 
        if self.useGlexec:
            command = '/opt/glexec/glexec-osg/sbin/glexec ' + self.condorTemp + '/' \
                      + self.glexecWrapper + ' ' + self.condorTemp + '/' + jdlFileName
            stdout, stdin, stderr = popen2.popen3(command)
        else:
            stdout, stdin, stderr = popen2.popen3('condor_submit '+submitOptions+jdlFileName)
 
        # Parse output, build numbers
        for line in stdout:
          matchObj = jobRegExp.match(line)
          if matchObj:
            ret_map[job['name']] = self.hostname + "//" + matchObj.group(2) + ".0"
            job.runningJob['schedulerId'] = ret_map[job['name']]
        try:
          jobName = ret_map[ job['name']  ]
          # This is a HACK for the server until query is passed the task
          #jobList = open('/tmp/'+taskId+'.lst', 'a')
          #jobList.write(jobName+'\n')
          #jobList.close()
        except KeyError:
          print "Job not submitted:"
          print stdout.readlines()
          print stderr.readlines()
        os.chdir(cacheDir)
        jobCount += 1
 
        if ce:
            job.runningJob['destination'] = ce.split(':')[0]
        else:
            job.runningJob['destination'] = execHost

    success = self.hostname

    return ret_map, taskId, success


  def findExecHost(self, requirements=''):
    if not requirements:
        return 'Unknown'
    jdlLines = requirements.split(';')
    execHost = 'Unknown'
    for line in jdlLines:
      if line.find("globusscheduler") != -1:
        parts = line.split('=')
        sched = parts[1]
        parts = sched.split(':')
        execHost = parts[0]

    return execHost.strip()


  def inputFiles(self,globalSandbox):
    filelist = ''
    if globalSandbox is not None:
      for file in globalSandbox.split(','):
        if file == '' :
            continue
        filename = os.path.abspath(file)
        filename.strip()
        filelist += filename + ','
    return filelist[:-1] # Strip off last ","


  def decode  ( self, obj, requirements='' ):
      """
      prepare file for submission
      """

      if type(obj) == RunningJob or type(obj) == Job :
          return self.singleApiJdl(obj, requirements)
      elif type(obj) == Task :
          return self.collectionApiJdl(obj, requirements)


  def singleApiJdl( self, job, requirements='' ):
      """
      build a job jdl
      """

      jdl  = ''
      jobId = int(job['jobId'])
      # Massage arguments into condor friendly (space delimited) form w/o backslashes
      jobArgs = job['arguments']
      jobArgs = jobArgs.replace(',',' ')
      jobArgs = jobArgs.replace('\\ ',',')
      jobArgs = jobArgs.replace('\\','')
      jobArgs = jobArgs.replace('"','')
      
      jdl += 'Arguments  = %s\n' % jobArgs
      if job['standardInput'] != '':
          jdl += 'input = %s\n' % job['standardInput']
      jdl += 'output  = %s\n' % job['standardOutput']
      jdl += 'error   = %s\n' % job['standardError']
      jdl += 'log     = %s.log\n' % os.path.splitext(job['standardError'])[0] # Same root at stderr
      jdl += 'stream_output = false\n'
      jdl += 'stream_error  = false\n'
      jdl += 'notification  = never\n'
      jdl += 'should_transfer_files   = YES\n'
      jdl += 'when_to_transfer_output = ON_EXIT\n'
      jdl += 'copy_to_spool           = false\n'
      
      # HACK: Remove if I can figure out where the request for .BrokerInfo is coming from
      outputFiles = []
      for fileName in job['outputFiles']:
          if not fileName.endswith('BrokerInfo'):
              outputFiles.append(fileName)
      if outputFiles:
          jdl += 'transfer_output_files   = ' + ','.join(outputFiles) + '\n'
      
      # Things in the requirements/jobType field
      jdlLines = requirements.split(';')
      ce = None
      for line in jdlLines:
        [key,value] = line.split('=',1)
        if key.strip() == "schedulerList":
          CEs = value.split(',')
          ceSlot = (jobId-1) // self.batchSize
          ceNum = ceSlot%len(CEs)
          ce = CEs[ceNum]
          jdl += "globusscheduler = " + ce + '\n'
        else:
          jdl += line.strip() + '\n';
            
      filelist = ''

      return jdl, filelist, ce


  def query(self, obj, service='', objType='node'):
    """
    query status of jobs
    """

    from xml.sax import make_parser
    from CondorHandler import CondorHandler
    from xml.sax.handler import feature_external_ges
    
    jobIds = {}
    bossIds = {}

    statusCodes = {'0':'RE', '1':'S', '2':'R',  # Convert Condor integer status
                   '3':'K',  '4':'D', '5':'A'} # to BossLite Status codes
    textStatusCodes = {
            '0':'Ready',
            '1':'Submitted',
            '2':'Running',
            '3':'Cancelled',
            '4':'Done',
            '5':'Aborted'
    }

    if type(obj) == Task: # and objType == 'node':
        taskId = obj['name']
        for job in obj.jobs:
            if not self.valid(job.runningJob):
                continue
            schedulerId = job.runningJob['schedulerId']

            bossIds[schedulerId] = {'status':'SD', 'statusScheduler':'Done'} # Done by default
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
      condor_status = {}
      cmd = 'condor_q -xml '
      if schedd != self.hostname:
        cmd += '-name ' + schedd + ' '
#      if not self.useGlexec:
#          cmd += os.environ['USER']
      cmd += """-constraint 'BLTaskID=="%s"'""" % taskId
    
      (input_file, output_fp) = os.popen4(cmd)

      # Throw away first three lines. Junk
      output_fp.readline()
      output_fp.readline()
      output_fp.readline()

      output_file = cStringIO.StringIO(output_fp.read())

      # If the command succeeded, close returns None
      # Otherwise, close returns the exit code
      if output_fp.close():
        raise SchedulerError("condor_q command failed.")
      
      handler = CondorHandler('GlobalJobId', ['JobStatus', 'GridJobId', 'MATCH_GLIDEIN_Gatekeeper', 'GlobalJobId'])
      parser = make_parser()
      parser.setContentHandler(handler)
      parser.setFeature(feature_external_ges, False)
      parser.parse(output_file)

      jobDicts = handler.getJobInfo()
            
      for globalJobId in jobDicts.keys():
        host,task,jobId = globalJobId.split("#")
        jobStatus = jobDicts[globalJobId].get('JobStatus',None)

        # Host can be either in GridJobId or Glidein match
        execHost = None
        gridJobId = jobDicts[globalJobId].get('GridJobId',None)
        if gridJobId:
          URI = gridJobId.split(' ')[1]
          execHost = URI.split(':')[0]
        glideinHost = jobDicts[globalJobId].get('MATCH_GLIDEIN_Gatekeeper',None)
        if glideinHost:
          execHost = glideinHost
      
        # Don't mess with jobs we're not interested in, put what we found into BossLite statusRecord
        if bossIds.has_key(schedd+'//'+jobId):
          statusRecord = {}
          statusRecord['status']          = statusCodes.get(jobStatus,'UN')
          statusRecord['statusScheduler'] = textStatusCodes.get(jobStatus,'Undefined')
          statusRecord['statusReason']    = ''
          statusRecord['service']         = service
          if execHost:
            statusRecord['destination']   = execHost

          bossIds[schedd+'//'+jobId] = statusRecord
      
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
      submitHost,jobId  = schedulerId.split('//')
      (input_file, output_file) = os.popen4("condor_rm -name  %s %s " % (submitHost,jobId))


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

      if outdir == '':
        outdir = obj['outputDirectory']

      for job in obj.jobs:
        if self.valid( job.runningJob ):
          self.getCondorOutput(job, outdir)

    # unknown object type
    else:
      raise SchedulerError('wrong argument type', str( type(obj) ))


  def getCondorOutput(self,job,outdir):
    fileList = []
    fileList.append(job['standardOutput'])
    fileList.append(job['standardError'])
    fileList.extend(job['outputFiles'])

    for file in fileList:
      try:
        shutil.move(self.condorTemp+'/'+file,outdir)
      except IOError:
        print "Could not move file ",file



  def postMortem( self, schedulerId, outfile, service):
    """
    Get detailed postMortem job info
    """

    if not outfile:
      raise SchedulerError('Empty filename',
                           'postMortem called with empty logfile name')

    submitHost,jobId  = schedulerId.split('//')
    cmd = "condor_q -l  -name  %s %s > %s" % (submitHost,jobId,outfile)
    return self.ExecuteCommand(cmd)


  def jobDescription( self, obj, requirements='', config='', service = '' ):
    """
    retrieve scheduler specific job description
    """

    return "Check jdl files in " + self.condorTemp + " after submit\n"
