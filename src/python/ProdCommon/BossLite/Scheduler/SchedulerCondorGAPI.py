#!/usr/bin/env python
"""
_SchedulerCondorGAPI_
"""

__revision__ = "$Id: SchedulerCondorGAPI.py,v 1.8 2008/03/31 12:15:49 ewv Exp $"
__version__ = "$Revision: 1.8 $"

import sys
import os
import popen2
import re
from socket import getfqdn

import traceback
import pdb
import pprint
import inspect

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
    self.hostname = getfqdn()
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


    print "Service =",service

    # better to use os join, etc.

    self.condorTemp = os.getcwd()+'/'+obj['scriptName'].split('/')[0]+'/share/.condor_temp'
    if os.path.isdir(self.condorTemp):
      pass
    else:
      os.mkdir(self.condorTemp)
    #      wms = service
    configfile = config
    # decode obj
    print 'CWD',os.getcwd()
    taskId = ''
    ret_map = {}

    jobRegExp = re.compile("\s*(\d+)\s+job\(s\) submitted to cluster\s+(\d+)*")

    if type(obj) == RunningJob or type(obj) == Job :
      jdl, sandboxFileList = self.decode( obj, requirements='' )
    elif type(obj) == Task :
      taskId = obj.data['name']
#      pdb.set_trace()
      for job in obj.getJobs():
        print "Job Object"#,job.getNodeName()
        job.runningJob['destination']="exec.host"
        jdl, sandboxFileList = self.decode( job, requirements='' )
        print "JobName",job['name'],  "mynode"
        jdl += "Queue 1\n"

#        print "Generated JDL\n",jdl

        stdout, stdin, stderr = popen2.popen3('condor_submit /home/ewv/test.jdl')
        for line in stdout:
          matchObj = jobRegExp.match(line)
          if matchObj:
#            print "Njobs =",matchObj.group(1)
#            print "jobID =",matchObj.group(2)
            ret_map[job['name']] = self.hostname + "//" + matchObj.group(2) + ".0"
        print job['name'],":",ret_map[ job['name']  ]


    #jdl, sandboxFileList = self.decode( obj, requirements='' )

    # return values

    # handle wms
    #      jdl, endpoints = self.mergeJDL( jdl, wms, configfile )

    # jdl ready!
    # print "Using jdl : \n" + jdl

    # installing a signal handler to clean files if the submission
    # is signaled e.g. for a timeout
    #      signal.signal(signal.SIGTERM, handler)

    # emulate ui round robin
    success = self.hostname
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
    #import Xml2Obj
    print "Service = ",service
    pprint.pprint(schedIdList)

    jobIds = {}
    bossIds = {}
    statusMap = {'I':'I', 'U':'RE', 'H':'SA', # Convert Condor status
                 'R':'R', 'X':'SK', 'C':'SD'} # to BossLite Status codes
    textStatusMap = {
            'I':'Scheduled',
            'R':'Running',
            'U':'Ready',
            'X':'Cancelled',
            'C':'Done',
            'H':'Aborted'
    }
    #Condor_q has an XML output mode that might be worth investigating.

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
      # call condor_q
      #cmd = 'condor_q -xml -name ' + schedd + ' ' + os.environ['USER']
      #(input_file, output_file) = os.popen4(cmd)
      #parser = Xml2Obj.Xml2Obj()
      #print "line:",output_file.readline()
      #print "line:",output_file.readline()
      #print "line:",output_file.readline()
      #topElement = parser.Parse(output_file)
      #for child in topElement.getElements(): # These are the job entries
        #for item in child.getElements():
          #pprint.pprint(inspect.getmembers(item))
          #dict = item.getAttributes()
          #if dict['a'] == 'JobStatus':
            #condor_status[

      cmd = 'condor_q -name ' + schedd + ' ' + os.environ['USER']
      (input_file, output_file) = os.popen4(cmd)

      # parse output and store Condor status in dictionary { 'id' : 'status' , ... }
      for line in output_file.readlines() :
        line = line.strip()
        try:
          line_array = line.split()
          if line_array[1].strip() == os.environ['USER'] :
            condor_status[line_array[0].strip()] = line_array[5].strip()
        except:
          pass

      # go through job_ids[schedd] and save status in bossIds
      # Each status record looks like this:
      # 'https://lb105.cern.ch:9000/tf9-in51MqEOzm_yTCvWEg': {'status': 'SU', 'statusScheduler': 'Submitted', 'destination': '', 'service': 'wms102.cern.ch', 'statusReason': ''}

      for id in jobIds[schedd] :
        for condor_id in condor_status.keys() :
          if condor_id.find(id) != -1:
            status = condor_status[condor_id]
            statusRecord = {}
            statusRecord['status']          = statusMap.get(status,'UN')
            statusRecord['statusScheduler'] = textStatusMap.get(status,'Undefined')
            statusRecord['statusReason']    = ''
            #statusRecord['destination']     = 'someHost'
            statusRecord['service']         = service

            bossIds[schedd+'//'+id] = statusRecord

    return bossIds

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
    print 'CWD',os.getcwd()

    for jobId in schedIdList:
      fileList = ['condor_g_'+str(jid)+'.log', 'BossOutArchive_'+str(jid)+'.tgz',
                  'condor_g_'+str(jid)+'.out', 'condor_g_'+str(jid)+'.err'] # Not correct names, how to get them?
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



