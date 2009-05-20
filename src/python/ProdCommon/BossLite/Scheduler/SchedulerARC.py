#!/usr/bin/env python
"""
_SchedulerARC_
"""

import sys  # Needed for anything else than debugging?

import os, time
#import socket
#import tempfile
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
import logging
import ldap
import re
#import arclib as arc

#
# Mapping from ARC status codes to BossLite dito.
#
# Meaning ARC status codes in comments below.
# BossLite status code docs:
# https://twiki.cern.ch/twiki/bin/view/CMS/BossLiteJob
#

StatusCodes = {
    "ACCEPTING": "SU ", # Job has reaced the CE
    "ACCEPTED":  "SU",  # Job submitted but not yet processed
    "PREPARING": "SW",  # Input files are being transferred
    "PREPARED":  "SW",  # Transferring input files done
    "SUBMITTING":"SR",  # Interaction with the LRMS at the CE ongoing
    "INLRMS:Q":  "SS",  # In the queue of the LRMS at the CE
    "INLRMS:R":  "R",   # Running
    "INLRMS:S":  "R",   # Suspended
    "INLRMS:E":  "R",   # About to finish in the LRMS
    "INLRMS:O":  "R",   # Other LRMS state
    "EXECUTED":  "R",   # Job is completed in the LRMS
    "FINISHING": "R",   # Output files are being transferred
    "KILLING":   "K",   # Job is being cancelled on user request
    "KILLED":    "K",   # Job canceled on user request
    "DELETED":   "A",   # Job removed due to expiration time
    "FAILED":    "DA",  # Job finished with an error.
    "FINISHED":  "SD",  # Job finished successfully.

    # In addition, let's define a few of our own
    "UNKNOWN":     "UN", # Job not known by ARC server (or, more typically, info.sys. too slow!)
    "WTF?":        "UN"  # Job not recognized as a job by the ARC client!
}


def ldapsearch(host, dn, filter, attr, retries=5):
     timeout = 30  # seconds

     for i in range(retries+1):
          try:
               if i > 0:
                    sys.stderr.write("Retrying ldapsearch ... (%i/%i)\n" % (i, retries))
                    time.sleep(i*15)

               con = ldap.initialize(host)      # host = ldap://hostname[:port]
               con.simple_bind_s()
               con.search(dn, ldap.SCOPE_SUBTREE, filter, attr)
               try:
                    x = con.result(all=1, timeout=timeout)[1]
               except ldap.SIZELIMIT_EXCEEDED:
                    # Apparently too much output. Let's try to get one entry at a time
                    # instead; that way we'll hopefully get at least a part of the
                    # total output.
                    sys.stderr.write("ldap.SIZELIMIT_EXCEEDED ...\n")
                    x = []
                    con.search(dn, ldap.SCOPE_SUBTREE, filter, attr)
                    tmp = con.result(all=0, timeout=timeout)
                    while tmp:
                         x.append(tmp[1][0])
                         try:
                              tmp = con.result(all=0, timeout=timeout)
                         except ldap.SIZELIMIT_EXCEEDED, e:
                              break;
               con.unbind()
               break;
          except ldap.LDAPError, e:
               con.unbind()
     else:
          raise e

     return x


def intersection(a, b):
    r = []
    for x in a:
        if x in b: r.append(x)
    return r


class SchedulerARC(SchedulerInterface):
    """
    basic class to handle ARC jobs
    """

    def __init__(self, **args):
        super(SchedulerARC, self).__init__(**args)
        self.vo = args.get("vo", "cms")


    def jobDescription (self, obj, requirements='', config='', service = ''):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        raise NotImplementedError

        
    def decode(self, job, task, requirements=''):
        """
        prepare scheduler specific job description

        used by self.submit(), return xrsl code.
        """
        xrsl = '&'
        xrsl += '(executable="%s")' % job['executable']

        # The comma separated list of input files contains '\"':s and '\':s
        # that should be removed -- otherwise the list will be split into
        # several arguments by the shell, which is WRONG!
        args = job['arguments'].replace('\\"', '').replace('\\', '')
        xrsl += '(arguments=%s)' % args

        xrsl += '(jobName="%s")' % job['name']
        xrsl += '(stdout="%s")' % job['standardOutput']
        xrsl += '(stderr="%s")' % job['standardError']
        if job['standardInput'] != '':
            xrsl += '(stdin="%s")' % job['standardInput']

        xrsl += '(inputFiles='
        for f in task['globalSandbox'].split(','):
            xrsl += '(%s %s)' % (f.split('/')[-1], f)
        xrsl += ')'

        if len(job['outputFiles']) > 0:
            xrsl += '(outputFiles='
            for f in job['outputFiles']:
                xrsl += '(%s "")' % f
            xrsl += ')'

        xrsl += requirements

        print xrsl
        return xrsl


    def submit (self, obj, requirements='', config='', service = ''):
        """
        set up submission parameters and submit
        uses self.decode()

        return jobAttributes, bulkId, service

        - jobAttributs is a map of the format
              jobAttributes[ 'name' : 'schedulerId' ]
        - bulkId is an eventual bulk submission identifier
        - service is a endpoit to connect withs (such as the WMS)
        """
        if type(obj) == Job:
            raise NotImplementedError
        elif type(obj) == Task:
            return self.submitTask(obj, requirements) 


    def submitTask(self, task, requirements=''):
        map = {}
        for job in task.getJobs():
            m, bulkId, service = self.submitJob(job, task, requirements)
            map.update(m)

        return map, bulkId, service


    def submitJob(self, job, task, requirements=''):

        xrsl = self.decode(job, task, requirements)
        command = "ngsub -e '%s'" % xrsl
        output, exitStat = self.ExecuteCommand(command)

        successfulMatch = re.match("Job submitted with jobid: +(\w+://([a-zA-Z0-9.]+)(:\d+)?(/.*)?/\d+)", output)
        if not successfulMatch or exitStat != 0:
            raise SchedulerError('Error in submit', output, command)

        jobId = successfulMatch.group(1)
        map={job['name'] : jobId}
        bulkId = job['taskId']
        ceName = successfulMatch.group(2)

        return map, bulkId, ceName


    def query(self, obj, service='', objType='node'):
        """
        Query status and eventually other scheduler related information,
        and store it in the job.runningJob data structure.

        It may use single 'node' scheduler id or bulk id for association

        """
        # FIXME: The test below was copied from the corresponding function
        # in SchedulerLsf.py. But isn't this function expected to work also
        # for type(obj) == Job ?
        if type(obj) != Task:
            raise SchedulerError('wrong argument type', str(type(obj)))

        # Running ngstat for many jobs at once might be faster. But
        # let's be lazy and take one at a time.
        for job in obj.jobs:

            if not self.valid(job.runningJob):
                continue
            
            jobid = str(job.runningJob['schedulerId']).strip()
            command = 'ngstat ' + jobid
            output, exitStat = self.ExecuteCommand(command)

            if exitStat != 0:
                raise SchedulerError('%i exit status for ngstat' % exitStat,
                                      output, command)

            arcStat = None
            host = None
            jobExitCode = None

            if output.find("Job information not found") >= 0:
                if output.find("job was only very recently submitted") >= 0:
                    arcStat = "ACCEPTING"  # At least approximately true
                else:
                    arcStat = "UNKNOWN"

                jobIdMatch = re.search("\w+://([a-zA-Z0-9.]+).*", output)
                if jobIdMatch:
                    host = jobIdMatch.group(1)

            elif output.find("Malformed URL:") >= 0:
                # This is something that really shoudln't happen.
                arcStat = "WTF?"
            else:

                # With special cases taken care of above, we are left with
                # "normal" jobs. They are assumed to have the format
                #
                # Job <jobId URL>
                #   Status: <status>
                #   Exit Code: <exit code>
                #
                # "Exit Code"-line might be missing.
                # Additional lines may exist, but we'll ignore them.

                for line in output.split('\n'):

                    jobIdMatch = re.match("Job +\w+://([a-zA-Z0-9.]+).*", line)
                    if jobIdMatch:
                        host = jobIdMatch.group(1)
                        continue
                        
                    statusMatch = re.match(" +Status: *(.+)", line)
                    if statusMatch:
                        arcStat = statusMatch.group(1)
                        continue
                        
                    codeMatch = re.match(" +Exit Code: *(\d+)", line)
                    if codeMatch:
                        jobExitCode = codeMatch.group(1)
                        continue

            if arcStat:
                job.runningJob['statusScheduler'] = arcStat
                job.runningJob['status'] = StatusCodes[arcStat]
            if host:
                job.runningJob['destination'] = host
            if jobExitCode:
                job.runningJob['wrapperReturnCode'] = jobExitCode

        return


    def getOutput(self, obj, outdir=''):
        """
        Get output files from jobs in 'obj' and put them in 'outdir', and  
        remove the job from the CE.
        """
        if type(obj) == Job:
            if not self.valid(obj.runningJob):
                raise SchedulerError('invalid object', str(obj.runningJob))

            self.getJobOutput(obj, outdir)
        elif type(obj) == Task:
            if outdir == '':
                outdir = obj['outputDirectory']

            for job in obj.jobs:
                if self.valid(job.runningJob):
                    self.getJobOutput(job, outdir)
        else:
            raise SchedulerError('wrong argument type', str(type(obj)))


    def getJobOutput(self, job, outdir):
        """
        Get output files from one job and put them in 'outdir', and  
        remove the job from the CE.
        """
        
        assert outdir != ''

        jobId = str(job.runningJob['schedulerId']).strip()

        if outdir[-1] != '/': outdir += '/'

        # Use ngcp + ngclean instead of ngget, because the latter always
        # puts the files under /somewhere/<NUMERICAL ID>, with the result
        # that we would have to move them afterwards. I feel this is more
        # elegant.
        cmd = 'ngcp %s/ %s' % (jobId, outdir)
        output, stat = self.ExecuteCommand(cmd)
        if stat != 0:
            raise SchedulerError('ngcp returned %i' % stat, output, cmd)

        cmd = 'ngclean %s' % jobId
        output, stat = self.ExecuteCommand(cmd)
        if stat != 0:
            raise SchedulerError('ngclean returned %i' % stat, output, cmd)


    def kill(self, obj):
        """
        Kill the job instance
        """
        if type(obj) == Job:
            jobList = [obj]
        elif type(obj) == Task:
            jobList = obj.jobs
        else:
            raise SchedulerError('wrong argument type', str(type(obj)))

        for job in jobList:
            if not self.valid(job.runningJob):
                raise SchedulerError('invalid object', str(job.runningJob))

            jobId = str(job.runningJob['schedulerId']).strip()
            cmd = "ngkill " + jobId
            output, stat = self.ExecuteCommand(cmd)

            if stat != 0:
                raise SchedulerError('ngkill returned %i' % stat, output, cmd)


    def postMortem (self, jobId, outfile, service):
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """
        cmd = "ngcat -l " + jobId + " > " + outfile
        return self.ExecuteCommand(cmd)[0]


    def matchResources(self, obj, requirements='', config='', service=''):
        """
        perform a resources discovery
        returns a list of resulting sites
        """
        raise NotImplementedError


    def parseGiisStr(self, giis_str):
        """
        Parse a giis string in either of the formats
            ldap://giis.csc.fi:2135/O=Grid/Mds-Vo-name=Finland
        or
            ldap://giis.csc.fi:2135/Mds-Vo-name=Finland,O=Grid
        and return the giis itself, and base, e.g.
        "ldap://giis.csc.fi:2135",  "Mds-Vo-name=Finland,O=Grid"
        """

        m = re.match("(ldap://[^/]*)/(.*)", giis_str)
        assert(m)

        giis = m.group(1)
        base_str = m.group(2)

        # If the 'base' part has the format 'y=b/x=a'
        # it has to be converted to 'x=a,y=b'. If it's in
        # the latter format already, we'll use it as it is.
        m = re.match("(.*=.*)/(.*=.*)", base_str)
        if m:
            base = m.group(2) + ',' + m.group(1)
        else:
            # FIXME: Check that base_str has some sane format
            # (e.g. "x=a,y=b")
            base = base_str

        return giis, base


    def getGiisStrList(self):
        """
        Find out which GIIS(s) to use
        """
        giises = []

        # First look in the file ~/.arc/client.conf
        if "HOME" in os.environ.keys():
            home = os.environ["HOME"]
            try:
                clientconf = open(home + "/.arc/client.conf", "r").readlines()
            except IOError:
                clientconf = []

            for line in clientconf:
                m = re.match("giis=\"*(ldap://[^\"]*)\"*", line)
                if m:
                    g = m.group(1)
                    giises.append(g)

        # Look for site-wide giislist
        try:
            arc_location = os.environ["NORDUGRID_LOCATION"]
        except KeyError:
            sys.stderr.write("ERROR: Environment variable NORDUGRID_LOCATION not set!\n")
            raise

        giislist = open(arc_location + "/etc/giislist", "r").readlines()
            
        for line in giislist:
            if line not in giises:
                giises.append(line)

        return giises


    def lcgInfo(self, tags, vos, seList=None, blacklist=None, whitelist=None, full=False):
        """
        Query grid information system for CE:s.
        Returns a list of resulting sites
        """

        # FIXME: Currently we ignore 'vos'!

        attr = [ 'nordugrid-cluster-name', 'nordugrid-cluster-localse',
                 'nordugrid-cluster-runtimeenvironment' ]

        giis_list = self.getGiisStrList()
        if not giis_list:
            raise SchedulerError("No GIISes?", "Something must be wrong with ARC's setup!")
        
        for giis_str in giis_list:
            giis, base = self.parseGiisStr(giis_str)
            try:
                ldap_result = ldapsearch(giis, base, '(objectClass=nordugrid-cluster)', attr, retries=2)
            except ldap.LDAPError, e:
                sys.stderr.write("WARNING: No reply from GIIS %s, trying another\n" % giis)
                pass
            else:
                break
        else:
            sys.stderr.write("ERROR: No more GIISes to try!  All GIISes down? Please wait for a while and try again\n")
            raise SchedulerError("No reply from GIISes", "")

        accepted_CEs = []
        for item in ldap_result:
            ce = item[1]
            name = ce['nordugrid-cluster-name'][0]
            localSEs = ce.get('nordugrid-cluster-localse', [])
            RTEs = ce.get('nordugrid-cluster-runtimeenvironment', [])

            if seList and not intersection(seList, localSEs):
                continue

            if tags and not intersection(tags, RTEs):
                continue

            if blacklist and name in blacklist:
                continue

            if whitelist and name not in whitelist:
                continue

            if full:
                accepted_CEs.append(name)
            else:
                return [ name ]

        return accepted_CEs
