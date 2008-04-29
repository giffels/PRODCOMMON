#!/usr/bin/env python
"""
_SchedulerCondor_
Scheduler class for vanilla Condor scheduler
"""

__revision__ = "$Id: SchedulerCondor.py,v 1.3 2008/04/29 08:15:42 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"

import re
import os
import popen2

from socket import getfqdn

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerCondor(SchedulerInterface) :
  """
  basic class to handle lsf jobs
  """
  def __init__( self, **args ):

    # call super class init method
    super(SchedulerCondor, self).__init__(**args)
    self.hostname = getfqdn()
    self.execDir = os.getcwd()+'/'
    self.workingDir = ''
    self.condorTemp = ''

  def checkUserProxy( self, cert='' ):
    return

  def jobDescription ( self, obj, requirements='', config='', service = '' ):
    """
    retrieve scheduler specific job description
    return it as a string
    """

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

    taskId = ''
    ret_map = {}
    scriptDir = os.path.split(obj['scriptName'])[0]
    self.workingDir = (os.sep).join(scriptDir.split(os.sep)[:-1])+os.sep
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
      raise NotImplementedError
    elif type(obj) == Task :
      taskId = obj['name']
      for job in obj.getJobs():
        requirements = obj['jobType']
        #execHost = self.findExecHost(requirements)
        #filelist = self.inputFiles(obj['globalSandbox'])
        #requirements += "transfer_input_files = " + filelist + '\n'
        job.runningJob['destination'] = self.hostname

        # Build JDL file
        jdl, sandboxFileList = self.decode( job, requirements)
        jdl += 'Executable = %s\n' % (obj['scriptName'])
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
        except KeyError:
          print "Job not submitted:"
          print stdout.readlines()
          print stderr.readlines()
        os.chdir(cacheDir)

    success = self.hostname
    success = self.hostname

    return ret_map, taskId, success

  def inputFiles(self,globalSandbox):
    #filelist = ''
    #if globalSandbox is not None :
      #for file in globalSandbox.split(','):
        #if file == '' :
            #continue
        #filename = os.path.abspath(file)
        #filename.strip()
        #filelist += filename + ','
    #return filelist[:-1]
    return ''

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
      jdl += 'Universe  = vanilla\n'
      jdl += 'environment = CONDOR_ID=$(Cluster).$(Process)\n'
      jdl += 'Arguments  = %s\n' % jobArgs
      if job['standardInput'] != '':
        jdl += 'input = %s\n' % job['standardInput']
      jdl += 'output  = %s\n' % job['standardOutput']
      jdl += 'error   = %s\n' % job['standardError']
      jdl += 'log     = %s.log\n' % os.path.splitext(job['standardError'])[0] # Same root at stderr
      jdl += 'stream_output = false\n'
      jdl += 'stream_error  = false\n'
      jdl += 'notification  = never\n'
      jdl += 'should_transfer_files   = NO\n'
      jdl += 'copy_to_spool           = false\n'
      jdl += 'transfer_output_files   = ' + ','.join(job['outputFiles']) + '\n'

      filelist = ''
      return jdl, filelist

  def query(self, schedIdList, service='', objType='node'):
    import SchedulerCondorCommon
    bossIds = SchedulerCondorCommon.query(schedIdList, service, objType)
    return bossIds

  def kill( self, schedIdList, service):
    """
    Kill jobs submitted to a given WMS. Does not perform status check
    """
    import SchedulerCondorCommon
    SchedulerCondorCommon.kill(schedIdList, service)

  def getOutput( self, obj, outdir='', service='' ):
    """
    Retrieve (move) job output from cache directory to outdir
    User files from CondorG appear asynchronously in the cache directory
    """
    import SchedulerCondorCommon
    SchedulerCondorCommon.getOutput(obj, outdir, service)

  def postMortem( self, schedulerId, outfile, service):
    """
    Get detailed postMortem job info
    """
    import SchedulerCondorCommon
    return SchedulerCondorCommon.postMortem( self, schedulerId, outfile, service)

  def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):
    """
    perform a resources discovery
    returns a list of resulting sites
    """

    return  seList
