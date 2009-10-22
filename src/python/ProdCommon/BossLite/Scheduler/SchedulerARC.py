#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Scheduler for the Nordugrid ARC middleware.
#
# Maintainers:
# Erik Edelmann <erik.edelmann@ndgf.fi>
# Jesper Koivumäki <jesper.koivumaki@hip.fi>
# 

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
import re, signal
#import arclib as arc

#
# Mapping from ARC status codes to BossLite dito.
#
# Meaning ARC status codes StatusReason table below.
# BossLite status code docs:
# https://twiki.cern.ch/twiki/bin/view/CMS/BossLiteJob
#

StatusCodes = {
    "ACCEPTING": "SU ",
    "ACCEPTED":  "SU",
    "PREPARING": "SW",
    "PREPARED":  "SW",
    "SUBMITTING":"SR",
    "INLRMS:Q":  "SS",
    "INLRMS:R":  "R",
    "INLRMS:S":  "R",
    "INLRMS:E":  "R",
    "INLRMS:O":  "R",
    "EXECUTED":  "R",
    "FINISHING": "R",
    "KILLING":   "K",
    "KILLED":    "K",
    "DELETED":   "A",
    "FAILED":    "DA",
    "FINISHED":  "SD",

    # In addition, let's define a few of our own
    "UNKNOWN":     "UN",
    "WTF?":        "UN"
}

StatusReason = {
    "ACCEPTING": "Job has reaced the CE",
    "ACCEPTED":  "Job submitted but not yet processed",
    "PREPARING": "Input files are being transferred",
    "PREPARED":  "Transferring input files done",
    "SUBMITTING":"Interaction with the LRMS at the CE ongoing",
    "INLRMS:Q":  "In the queue of the LRMS at the CE",
    "INLRMS:R":  "Running",
    "INLRMS:S":  "Suspended",
    "INLRMS:E":  "About to finish in the LRMS",
    "INLRMS:O":  "Other LRMS state",
    "EXECUTED":  "Job is completed in the LRMS",
    "FINISHING": "Output files are being transferred",
    "KILLING":   "Job is being cancelled on user request",
    "KILLED":    "Job canceled on user request",
    "DELETED":   "Job removed due to expiration time",
    "FAILED":    "Job finished with an error.",
    "FINISHED":  "Job finished successfully.",

    "UNKNOWN":    "Job not known by ARC server (or info.sys. too slow!)",
    "WTF?":       "Job not recognized as a job by the ARC client!"
}


def count_nonempty(list):
    """Count number of non-empty items"""
    n = 0
    for i in list:
        if i: n += 1
    return n


class TimeoutFunctionException(Exception): 
    """Exception to raise on a timeout""" 
    pass 


class TimeoutFunction: 
    def __init__(self, function, timeout): 
        self.timeout = timeout 
        self.function = function 

    def handle_timeout(self, signum, frame): 
        raise TimeoutFunctionException()

    def __call__(self, *args): 
        old = signal.signal(signal.SIGALRM, self.handle_timeout) 
        signal.alarm(self.timeout) 
        try: 
            result = self.function(*args)
        finally: 
            signal.signal(signal.SIGALRM, old)
        signal.alarm(0)
        return result 


def get_ngsub_opts(xrsl):
    """
    If the xrsl-code contains (cluster=...), we can speed up submitting a lot by using option '-c ...' to ngsub
    """
    opt = ""
    clusters = []
    for attr in xrsl.split(')('):
        m = re.match(".*cluster=([^)]*)", attr)
        if m and m.group(1) not in clusters:
            opt += " -c " + m.group(1)
            clusters.append(m.group(1))
    return opt


def ldapsearch(host, dn, filter, attr, logging, scope=ldap.SCOPE_SUBTREE, retries=5):
     timeout = 45  # seconds

     for i in range(retries+1):
          try:
               if i > 0:
                    logging.info("Retrying ldapsearch ... (%i/%i)" % (i, retries))
                    time.sleep(i*10)

               con = ldap.initialize(host)      # host = ldap://hostname[:port]
               bind = TimeoutFunction(con.simple_bind_s, timeout)
               try:
                   bind()
               except TimeoutFunctionException:
                   raise ldap.LDAPError("Bind timeout")
               con.search(dn, scope, filter, attr)
               try:
                   x = con.result(all=1, timeout=timeout)[1]
               except ldap.SIZELIMIT_EXCEEDED:
                    # Apparently too much output. Let's try to get one
                    # entry at a time instead; that way we'll hopefully get
                    # at least a part of the total output.
                    logging.info("ldap.SIZELIMIT_EXCEEDED ...")
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


def parseGiisUrl(giis_url):
    """
    Parse a giis string in either of the formats
        ldap://giis.csc.fi:2135/O=Grid/Mds-Vo-name=Finland
    or
        ldap://giis.csc.fi:2135/Mds-Vo-name=Finland,O=Grid
    and return the giis itself, and base, e.g.
    "ldap://giis.csc.fi:2135",  "Mds-Vo-name=Finland,O=Grid"
    """

    m = re.match("(ldap://[^/]*)/(.*)", giis_url)
    assert(m)

    host = m.group(1)
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

    return host, base


def getGiisUrlList():
    """
    Find out which GIIS(s) to use
    """
    giises = []

    # 
    # FIXME: Maybe we could just parse the output of 'ngtest -O'?
    #

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
        self.logging.error("Environment variable NORDUGRID_LOCATION not set!")
        raise

    giislist = open(arc_location + "/etc/giislist", "r").readlines()
            
    for line in giislist:
        if line not in giises:
            giises.append(line)

    return giises


class SchedulerARC(SchedulerInterface):
    """
    basic class to handle ARC jobs
    """

    def __init__(self, **args):
        super(SchedulerARC, self).__init__(**args)
        self.vo = args.get("vo", "cms")
        self.giis_result = {}
        self.ce_result = {}
        self.user_xrsl = args.get("user_xrsl", "")
        self.scheduler = "ARC"


    def jobDescription(self, obj, requirements='', config='', service = ''):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        assert type(obj) == Task

        xrsl = "+"
        for job in obj.getJobs():
            xrsl += '(' +  self.decode(job, obj, requirements) + ')'
        return xrsl


        
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

        inputfiles = ""
        xrsl += '(inputFiles='
        for f in task['globalSandbox'].split(','):
            xrsl += '(%s %s)' % (f.split('/')[-1], f)
            inputfiles += "\\ " + f.split('/')[-1]
        xrsl += ')'

        outputfiles = ""
        if len(job['outputFiles']) > 0:
            xrsl += '(outputFiles='
            for f in job['outputFiles']:
                xrsl += '(%s "")' % f
                outputfiles += "\\ " + f
            xrsl += ')'

        xrsl += "(environment="
        # Provide a list of the in- and outputfiles in environment
        # variables. The '\:s' between the filenames are
        # there to avoid confusing the the shell while interpreting the 
        # 'ngsub # -e <xrsl-code>' command
        xrsl += "(ARC_INPUTFILES \"%s\")(ARC_OUTPUTFILES \"%s\")" % (inputfiles, outputfiles)
        xrsl += "(ARC_STDOUT %s)(ARC_STDERR %s)" % (job['standardOutput'], job['standardError'])
        xrsl += ')'

        xrsl += requirements

        # User supplied thingies:
        xrsl += self.user_xrsl
        for s in task['jobType'].split('&&'):
            if re.match('^ *\(.*=.*\) *$', s):
                xrsl += s

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
            map = {}
            for job in obj.getJobs():
                m, bulkId = self.submitJob(obj, job, requirements)
                map.update(m)
            return map, bulkId, service 


    def submitJob(self, task, job, requirements):

        # Build xRSL & cmdline-options
        xrsl = self.decode(job, task, requirements)
        opt = get_ngsub_opts(xrsl)

        # Submit
        command = "ngsub -e '%s' %s" % (xrsl, opt)
        self.logging.debug(command)
        self.setTimeout(300)
        output, exitStat = self.ExecuteCommand(command)
        if exitStat != 0:
            raise SchedulerError('Error in submit', output, command)

        # Parse output of submit command
        match = re.match("Job submitted with jobid: +(\w+://([a-zA-Z0-9.]+)(:\d+)?(/.*)?/\d+)", output)
        if not match:
            raise SchedulerError('Error in submit', output, command)

        arcId = match.group(1)
        m = {job['name']: arcId}  # arcId will end up in job.runningJob['schedulerId']

        self.logging.info("Submitted job with id %s" % arcId)

        return m, job['taskId']


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
                if not job.runningJob['schedulerId']:
                    self.logging.warning("job %s has no schedulerId!" % job['name'])
                self.logging.debug("job invalid: schedulerId = %s"%str(job.runningJob['schedulerId']))
                self.logging.debug("job invalid: closed = %s" % str(job.runningJob['closed']))
                self.logging.debug("job invalid: status = %s" % str(job.runningJob['status']))
                continue
            
            arcId = job.runningJob['schedulerId']
            self.logging.debug('Querying job %s with arcId %s' % (job['name'], arcId))

            cmd = 'ngstat ' + arcId
            output, stat = self.ExecuteCommand(cmd)

            if stat != 0:
                raise SchedulerError('%i exit status for ngstat' % stat, output, cmd)

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
                job.runningJob['statusReason'] = StatusReason[arcStat]
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

        arcId = job.runningJob['schedulerId']
        self.logging.debug('Getting job %s with arcId %s' % (job['name'], arcId))

        if outdir[-1] != '/': outdir += '/'

        # Use ngcp + ngclean instead of ngget, because the latter always
        # puts the files under /somewhere/<NUMERICAL ID>, with the result
        # that we would have to move them afterwards. I feel this is more
        # elegant.
        cmd = 'ngcp %s/ %s' % (arcId, outdir)
        output, stat = self.ExecuteCommand(cmd)
        if stat != 0:
            raise SchedulerError('ngcp returned %i' % stat, output, cmd)

        cmd = 'ngclean %s' % arcId
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
            
            arcId = job.runningJob['schedulerId']
            self.logging.debug('Killing job %s with arcId %s' % (job['name'], arcId))

            cmd = "ngkill " + arcId
            output, stat = self.ExecuteCommand(cmd)

            if stat != 0:
                raise SchedulerError('ngkill returned %i' % stat, output, cmd)


    def postMortem (self, arcId, outfile, service):
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """
        self.logging.debug('postMortem for job %s' % arcId)
        cmd = "ngcat -l " + arcId + " > " + outfile
        return self.ExecuteCommand(cmd)[0]


    def matchResources(self, obj, requirements='', config='', service=''):
        """
        perform a resources discovery
        returns a list of resulting sites
        """
        raise NotImplementedError


    def query_giis(self, giises):
        """
        Return CEs and sub-GIISes from the first GIIS in
        'giises'-list that replies.
        """

        attr = [ 'giisregistrationstatus' ]

        for giis in giises:
            # Use cached result if we have it:
            if giis['host'] in self.giis_result.keys():
                ldap_result = self.giis_result[giis['host']]
                break

            try:
                self.logging.info("Using GIIS %s, %s" % (giis['host'], giis['base']))
                ldap_result = ldapsearch(giis['host'], giis['base'], '(objectclass=*)', attr, self.logging, scope=ldap.SCOPE_BASE, retries=0)
            except ldap.LDAPError:
                self.logging.warning("No reply from GIIS %s, trying another" % giis['host'])
                pass
            else:
                self.giis_result[giis['host']] = ldap_result
                break
        else:
            self.logging.error("No more GIISes to try!  All GIISes down? Please wait for a while and try again")
            raise SchedulerError("No reply from GIISes", "")

        CEs = []
        giises = []
        for r in ldap_result:
            item = r[1]

            if item['Mds-Reg-status'][0] != 'VALID':
                continue

            m_ce = re.match("nordugrid-cluster-name=", item['Mds-Service-Ldap-suffix'][0])
            m_giis = re.match("Mds-Vo-name=.*, *[oO]=[gG]rid", item['Mds-Service-Ldap-suffix'][0])

            if m_ce:
                CEs.append({'name': item['Mds-Service-hn'][0], 'port': item['Mds-Service-port'][0]})
            elif m_giis:
                giises.append({'name': item['Mds-Service-hn'][0], 'port': item['Mds-Service-port'][0],
                               'base':item['Mds-Service-Ldap-suffix'][0]})
        return CEs, giises


    def check_CEs(self, CEs, tags, vos, seList, blacklist, whitelist, full):
        """
        Return those CEs that fullfill requirements.
        """

        accepted_CEs = []

        attr = ['nordugrid-cluster-name', 'nordugrid-cluster-localse',
                'nordugrid-cluster-runtimeenvironment' ]

        for ce in CEs:
            if ce['name'] in self.ce_result.keys():
                ldap_result = self.ce_result[ce['name']]
            else:
                host = 'ldap://' + ce['name'] + ':' + ce['port']
                try:
                    ldap_result = ldapsearch(host,'mds-vo-name=local,o=grid','objectclass=nordugrid-cluster', attr, self.logging, retries=0)
                    self.ce_result[ce['name']] = ldap_result
                except ldap.LDAPError:
                    continue

            if not ldap_result:
                continue

            ce = ldap_result[0][1]
            name = ce['nordugrid-cluster-name'][0]
            localSEs = set(ce.get('nordugrid-cluster-localse', []))
            RTEs = set(ce.get('nordugrid-cluster-runtimeenvironment', []))

            if count_nonempty(seList) > 0 and not set(seList) & localSEs:
                continue

            if count_nonempty(tags) > 0 and not set(tags) <= RTEs:
                continue

            if count_nonempty(blacklist) > 0 and name in blacklist:
                continue

            if count_nonempty(whitelist) > 0 and name not in whitelist:
                continue

            accepted_CEs.append(name)
            #if not full:
            #    break

        return accepted_CEs


    def pick_CEs_from_giis_trees(self, roots, tags, vos, seList, blacklist, whitelist, full):
        """
        Recursively traverse the GIIS tree, starting from the first 'root' that replies;
        return CEs fullfilling requirements.
        """

        CEs, giises = self.query_giis(roots)
        accepted_CEs = self.check_CEs(CEs, tags, vos, seList, blacklist, whitelist, full)

        if len(accepted_CEs) > 0 and not full:
            return accepted_CEs

        for g in giises:
            host = 'ldap://' + g['name'] + ':' + g['port']
            root = {'host':host, 'base': g['base']}
            accepted_CEs += self.pick_CEs_from_giis_trees([root], tags, vos, seList, blacklist, whitelist, full)
            if len(accepted_CEs) > 0 and not full:
                break

        return accepted_CEs



    def lcgInfo(self, tags, vos, seList=None, blacklist=None, whitelist=None, full=False):
        """
        Query grid information system for CE:s.
        Returns a list of resulting sites (or the first one, if full == False)
        """
        # FIXME: Currently we ignore 'vos'!

        self.logging.debug("lcgInfo called with %s, %s, %s, %s, %s, %s" % (str(tags), str(vos), str(seList), str(blacklist), str(whitelist), str(full)))

        if type(full) == type(""):  
            full = (full == "True")

        giis_urls = getGiisUrlList()

        if not giis_urls:
            raise SchedulerError("No Toplevel GIISes?", "Something must be wrong with ARC's setup!")
    
        tolevel_giises = []
        for g in giis_urls:
            host, base = parseGiisUrl(g)
            tolevel_giises.append({'host': host, 'base': base})

        accepted_CEs = self.pick_CEs_from_giis_trees(tolevel_giises, tags, vos, seList, blacklist, whitelist, full)

        return accepted_CEs

