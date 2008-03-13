#!/usr/bin/env python
"""
_SchedulerCondorGAPI_
"""

__revision__ = "$Id: SchedulerCondorGAPI.py,v 1.2 2008/03/12 15:32:15 ewv Exp $"
__version__ = "$Revision: 1.2 $"

import sys
import os
import popen2
import traceback
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerCondorGAPI(SchedulerInterface) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, user_proxy = '' ):

    # call super class init method
    super(SchedulerCondorGAPI, self).__init__(user_proxy)

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

    #      wms = service
    configfile = config
    # decode obj
    jdl, sandboxFileList = self.decode( obj, requirements='' )

    # return values
    taskId = ''
    ret_map = {}

    # handle wms
    #      jdl, endpoints = self.mergeJDL( jdl, wms, configfile )

    # jdl ready!
    # print "Using jdl : \n" + jdl

    # installing a signal handler to clean files if the submission
    # is signaled e.g. for a timeout
    #      signal.signal(signal.SIGTERM, handler)

    jdl.append("Queue 1\n");



    # emulate ui round robin
    #success = ''
    #seen = []
    #for wms in endpoints :
        #try :
            #wms = wms.replace("\"", "").strip()
            #if  len( wms ) == 0 or wms[0]=='#' or wms in seen:
                #continue
            #else :
                #seen.append( wms)
            #print "Submitting to : " + wms
            #taskId, ret_map = \
                    #self.wmproxySubmit( jdl, wms, sandboxFileList )
            #success = wms
            #break
        #except SchedulerError, err:
            #print err
            #continue

    # clean files
    #os.system("rm -rf " +  self.SandboxDir + ' ' + self.zippedISB)

    return ret_map, taskId, success

  def decode  ( self, obj, requirements='' ):
      """
      prepare file for submission
      """
      if type(obj) == RunningJob or type(obj) == Job :
          return self.singleApiJdl ( obj, requirements='' )
      elif type(obj) == Task :
          return self.collectionApiJdl ( obj, requirements='' )

  def singleApiJdl( self, job, requirements='' ):
      """
      build a job jdl easy to be handled by the wmproxy API interface
      and gives back the list of input files for a better handling
      """

      # general part
      jdl = ""
      jdl += 'Executable = "%s";\n' % job[ 'executable' ]
      jdl += 'Universe   = globus\n'
      jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
      if job[ 'standardInput' ] != '':
          jdl += 'input = "%s";\n' % job[ 'standardInput' ]
      jdl += 'output  = "%s";\n' % job[ 'standardOutput' ]
      jdl += 'error   = "%s";\n' % job[ 'standardError' ]

      #print CMD ("globusscheduler         = $globusscheduler\n");
      #if ( ! ($globusrsl eq "") ) {
      #    print CMD ("globusrsl               = $globusrsl\n");
      #}
      # output,error files passed to executable
      # print CMD ("initialdir              = $subdir\n");
      jdl += 'stream_output           = false;\n'
      jdl += 'stream_error            = false;\n'
      jdl += 'notification            = never;\n'
      # Condor log file
      #    print CMD ("log                     = $condorlog\n");
      # Transfer files
      jdl += 'should_transfer_files   = YES;\n'
      jdl += 'when_to_transfer_output = ON_EXIT;\n'
      #    print CMD ("transfer_input_files    = $inSandBox\n");
      jdl += 'copy_to_spool           = fals;\n'
      #    print CMD ("transfer_output_files   = $outSandBox\n");
      # A string to help finding boss jobs in condor

      filelist = ''
      return jdl, filelist

  def query(self, schedIdList, service='', objType='node'):
    """
    query status of jobs
    """
    jobIds = {}
    bossIds = {}

    #Condor_q has an XML output mode that might be worth investigating.

    for id in schedIdList:
      boss_ids[service+'//'+id] = 'SD' # Done by default?
        # extract schedd and id from bossId
      schedd = service     # id.split('//')[0]
        #id     = id.split('//')[1]
        # fill dictionary
      if schedd in job_ids.keys() :
        jobIds[schedd].append(id)
      else :
        jobIds[schedd] = [id]

    for schedd in jobIds.keys() :

      #if logFile :
          #logFile.write(schedd+'\n')

      # call condor_q
      cmd = 'condor_q -name ' + schedd + ' ' + os.environ['USER']
      (input_file, output_file) = os.popen4(cmd)

      # parse output and store Condor status in dictionary { 'id' : 'status' , ... }
      condor_status = {}
      for line in output_file.readlines() :
        line = line.strip()
        #if logFile :
          #logFile.write(line+'\n')
        try:
          line_array = line.split()
          if line_array[1].strip() == os.environ['USER'] :
            condor_status[line_array[0].strip()] = line_array[5].strip()
        except:
          pass

      # go through job_ids[schedd] and save status in boss_ids
      # Would should do this with a map
      for id in job_ids[schedd] :
        for condor_id in condor_status.keys() :
          if condor_id.find(id) != -1 :
            status = condor_status[condor_id]
            #if logFile :
                #logFile.write(status+'\n')
            if status == 'I':
              bossIds[schedd+'//'+id] = 'I'
            elif status == 'U':
              bossIds[schedd+'//'+id] = 'RE'
            elif status == 'H':
              bossIds[schedd+'//'+id] = 'SA'
            elif status == 'R':
              bossIds[schedd+'//'+id] = 'R'
            elif status == 'X':
              bossIds[schedd+'//'+id] = 'SK'
            elif status == 'C':
              bossIds[schedd+'//'+id] = 'SD'
            else:
              bossIds[schedd+'//'+id] = 'UN'

  def kill( self, schedIdList, service):
    """
    Kill jobs submitted to a given WMS. Does not perform status check
    """

    # Do killCheck too?

    host = service

    if len(host) == 0 :
        return
    for jobId in schedIdList:
      # Use ExecuteCommand?
      killer = popen2.Popen4("condor_rm -name  %s %s " % (service,jobId))
      exitStatus = killer.wait()
      content = killer.fromchild.read()

    """Perl code snippet

    chomp $identifier;
    $status="error";
    # use GlobalJobId to query for schedd name
    if ( $identifier =~ /(.+)\/\/(.+)/ ) {
        $schedd=$1;
        $sid=$2;
    }
    $killcmd = "condor_rm -name $schedd $sid 2>&1  |";
    if ( LOG ) {
        print LOG "\n====>> Kill request for job $sid\n";
        print LOG "Killing with command $killcmd\n";
        print LOG "*** Start dump of kill request:\n";
    }
    open (KILL, $killcmd);
    while (<KILL>) {
        if ($_ =~ m/Cluster\s+.+\s+has been marked for removal/) {
            $status = "killed";
        }
    """

  def getOutput( self, schedIdList,  outdir, service ):
    """
    Retrieve (move) job output from cache directory to outdir
    User files from CondorG appear asynchronously in the cache directory
    """

    import shutil

    for jobId in schedIdList:
      fileList = ['condor_g_'+str(jid)+'.log', 'BossOutArchive_'+str(jid)+'.tgz',
                  'condor_g_'+str(jid)+'.out', 'condor_g_'+str(jid)+'.err', # Not correct names, how to get them?
      for file in fileList:
        try :
          shutil.move(file,outdir)
        except BaseException, err:
          print "Problem retrieving file "+file+": " + err.toString()

  def postMortem( self, schedulerId, outfile, service):
        """
        Get detailed postMortem job info
        """

        cmd = 'condor_q -l -name ' + service + ' ' + schedulerId

        return self.ExecuteCommand(cmd)



