#!/usr/bin/env python
"""
_SchedulerCondor_
Scheduler class for vanilla Condor scheduler
"""

__revision__ = "$Id: SchedulerCondor.py,v 1.5 2008/04/29 16:33:55 ewv Exp $"
__version__ = "$Revision: 1.5 $"

import re
import os
import popen2

from socket import getfqdn

from SchedulerCondorCommon import SchedulerCondorCommon
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerCondor(SchedulerCondorCommon) :
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

  def findExecHost(self, requirements=''):
    return self.hostname

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

  def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):
    """
    perform a resources discovery
    returns a list of resulting sites
    """

    return  seList