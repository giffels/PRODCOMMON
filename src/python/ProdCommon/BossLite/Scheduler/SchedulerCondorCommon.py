#!/usr/bin/env python
"""
_SchedulerCondorCommon_
Base class for CondorG and GlideIn schedulers
"""

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.5 2008/04/18 18:25:46 ewv Exp $"
__version__ = "$Revision: 1.5 $"

# For earlier history, see SchedulerCondorGAPI.py

import sys
import os
import popen2
import re
import shutil

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
  def __init__( self, user_proxy = '' ):

    # call super class init method
    super(SchedulerCondorCommon, self).__init__(user_proxy)
    self.hostname = getfqdn()
    self.execDir = os.getcwd()+'/'
    self.workingDir = ''
    self.condorTemp = ''

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


    # Figure out our environment, make some directories

    #self.execDir = os.getcwd()+'/'
    # better to use os join, etc.
    self.workingDir = self.execDir+obj['scriptName'].split('/')[0]+'/'
    self.condorTemp = self.workingDir+'share/.condor_temp'
    if os.path.isdir(self.condorTemp):
      pass
    else:
      os.mkdir(self.condorTemp)

    configfile = config

    taskId = ''
    ret_map = {}

    jobRegExp = re.compile("\s*(\d+)\s+job\(s\) submitted to cluster\s+(\d+)*")

    if type(obj) == RunningJob or type(obj) == Job :
      jdl, sandboxFileList = self.decode( obj, requirements='' )
    elif type(obj) == Task :
      taskId = obj['name']
      for job in obj.getJobs():
        requirements = obj['jobType']
        execHost = self.findExecHost(requirements)
        filelist = self.inputFiles(obj['globalSandbox'])
        requirements += "transfer_input_files = " + filelist + '\n'
        job.runningJob['destination'] = execHost

        # Build JDL file
        jdl, sandboxFileList = self.decode( job, requirements)
        jdl += 'Executable = %s\n' % (self.execDir+obj['scriptName'])
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
        stdout, stdin, stderr = popen2.popen3('condor_submit '+jdlFileName)

        # Parse output, build numbers
        for line in stdout:
          matchObj = jobRegExp.match(line)
          if matchObj:
            ret_map[job['name']] = self.hostname + "//" + matchObj.group(2) + ".0"
            job.runningJob['schedulerId'] = ret_map[job['name']]
        try:
          junk = ret_map[ job['name']  ]
        except: #FIXME: Which exception? KeyError?
          print "Job not submitted:"
          print stdout.readlines()
          print stderr.readlines()
        os.chdir(cacheDir)

    success = self.hostname

    return ret_map, taskId, success

  def findExecHost(self, requirements=''):
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
    if globalSandbox is not None :
      for file in globalSandbox.split(','):
        if file == '' :
            continue
        filename = os.path.abspath(file)
        filename.strip()
        filelist += filename + ','
    return filelist[:-1]

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

      jdl += 'stream_output = false\n'
      jdl += 'stream_error  = false\n'
      jdl += 'notification  = never\n'
      # Condor log file
      #    print CMD ("log                     = $condorlog\n");
      jdl += 'should_transfer_files   = YES\n'
      jdl += 'when_to_transfer_output = ON_EXIT\n'
      jdl += 'copy_to_spool           = false\n'
      jdl += 'transfer_output_files   = ' + ','.join(job['outputFiles']) + '\n'

      # Things in the requirements/jobType field
      jdlLines = requirements.split(';')
      for line in jdlLines:
        jdl += line.strip() + '\n';

      filelist = ''
      return jdl, filelist

  def query(self, schedIdList, service='', objType='node'):
    """
    query status of jobs
    """
    from xml.dom.minidom import parse

    # HACK: Don't know how to solve this one. When I kill jobs they are set to "K" but since
    # CondorG cancelled jobs leave the queue, they are in the same state as "Done" jobs, so
    # crab -status eventually shows them as "Done"

    jobIds = {}
    bossIds = {}

    statusCodes = {'0':'RE', '1':'SS', '2':'R',  # Convert Condor integer status
                   '3':'SK', '4':'SD', '5':'SA'} # to BossLite Status codes
    textStatusCodes = {
            '0':'Ready',
            '1':'Scheduled',
            '2':'Running',
            '3':'Cancelled',
            '4':'Done',
            '5':'Aborted'
    }

    # Get a list of the schedd's that were used to submit this task
    for id in schedIdList:

      bossIds[id] = {'status':'SD','statusScheduler':'Done'} # Done by default?
      schedd = id.split('//')[0]
      job    = id.split('//')[1]
        # fill dictionary
      if schedd in jobIds.keys():
        jobIds[schedd].append(job)
      else :
        jobIds[schedd] = [job]

    for schedd in jobIds.keys() :
      condor_status = {}
      cmd = 'condor_q -xml -name ' + schedd + ' ' + os.environ['USER']
      (input_file, output_file) = os.popen4(cmd)

      # Throw away first three lines. Junk
      output_file.readline()
      output_file.readline()
      output_file.readline()

      # Start parsing XML
      dom = parse(output_file)
      classAd = dom.getElementsByTagName("classads")[0]
      jobList = classAd.getElementsByTagName("c")  # Jobs are "c" elements
      for job in jobList:
        globalJobId = ''
        jobId = 0
        jobStatus = None
        gridJobId = None
        execHost = None
        adList = job.getElementsByTagName("a")     # Job attributes are "a" elements
        for ad in adList:
          name = ad.getAttribute('n')
          if name=="JobStatus":
            jobStatus = (ad.getElementsByTagName("i")[0]).firstChild.data
          if name=="GlobalJobId":
            globalJobId = (ad.getElementsByTagName("s")[0]).firstChild.data
            host,task,jobId = globalJobId.split("#")
          if name=="GridJobId":
            gridJobId = (ad.getElementsByTagName("s")[0]).firstChild.data
            URI = gridJobId.split(' ')[1]
            execHost = URI.split(':')[0]
          if name=="MATCH_GLIDEIN_Site":
            execHost = (ad.getElementsByTagName("s")[0]).firstChild.data

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

    return bossIds

  def kill( self, schedIdList, service):
    """
    Kill jobs submitted to a given WMS. Does not perform status check
    """

    for job in schedIdList:
      submitHost,jobId  = job.split('//')
      (input_file, output_file) = os.popen4("condor_rm -name  %s %s " % (submitHost,jobId))

  def getOutput( self, obj, outdir='', service='' ):
    """
    Retrieve (move) job output from cache directory to outdir
    User files from CondorG appear asynchronously in the cache directory
    """

    #self.execDir = os.getcwd()+'/'
    self.workingDir = self.execDir+obj['scriptName'].split('/')[0]+'/'
    self.condorTemp = self.workingDir+'share/.condor_temp'

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
    fullFilename = outfile+'.LoggingInfo'
    cmd = "condor_q -l  -name  %s %s > %s" % (submitHost,jobId,fullFilename)
    return self.ExecuteCommand(cmd)
