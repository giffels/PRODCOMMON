#!/usr/bin/env python
"""
_SchedulerCondorCommon_
Base class for CondorG and GlideIn schedulers
"""

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.10 2008/04/28 19:51:58 ewv Exp $"
__version__ = "$Revision: 1.10 $"

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

import pdb           # FIXME: Remove
import pprint        # FIXME: Remove
import inspect       # FIXME: Remove

class SchedulerCondorCommon(SchedulerInterface) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, user_proxy = '' ):

    # call super class init method
    super(SchedulerCondorCommon, self).__init__(user_proxy)
    #self.hostname = getfqdn()
    #self.execDir = os.getcwd()+'/'
    #self.workingDir = ''
    #self.condorTemp = ''

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
