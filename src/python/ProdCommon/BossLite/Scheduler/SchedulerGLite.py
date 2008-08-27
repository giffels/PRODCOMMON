#!/usr/bin/env python
"""
basic glite CLI interaction class
"""


__revision__ = "$Id: SchedulerGLite.py,v 1.3 2008/05/16 14:44:52 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"

import sys
import os
import traceback
import tempfile
from BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from BossLite.Common.Exceptions import SchedulerError
from BossLite.DbObjects.Job import Job
from BossLite.DbObjects.Task import Task
from BossLite.DbObjects.RunningJob import RunningJob
#
# Import gLite specific modules
try:
    from glite_wmsui_LbWrapper import Status
    import Job
    from BossLite.Scheduler.GLiteLBQuery import checkJobs, checkJobsBulk, \
         groupByWMS
except:
    err = \
        """
        missing glite environment.
        Try export PYTHONPATH=$PYTHONPATH:$GLITE_LOCATION/lib
        """
    raise ImportError(err)
#
#
def getChildrens( taskId ) :
    """
    function to retrieve job,gridid asssociation
    """
    
    error = ""
    ret_map = {}
    status = Status()
    jobStatus = Job.JobStatus (status)
    states = jobStatus.states_names
    status.getStatus(taskId, 0)
    err, apiMsg = status.get_error()
    if err:
        raise SchedulerError( err, apiMsg )
    jobidInfo = status.loadStatus(0)
    VECT_OFFSET = jobStatus.ATTR_MAX
    intervals = int ( len(jobidInfo) / VECT_OFFSET )
    wms = jobidInfo[ states.index( 'Network server' ) ]
    id_index = states.index( 'User tags' )
    gid_index = states.index( 'Jobid' )
    if jobidInfo[ VECT_OFFSET + id_index ] == '' :
        raise SchedulerError( "Error", "Wait a bit" )
    for off in range ( 1, intervals ):
        offset = off * VECT_OFFSET
        bossid = jobidInfo[ offset + id_index ]
        bossid = bossid[ bossid.find('=')+1: bossid.find(';') ]
        ret_map[ bossid ] = jobidInfo[ offset + gid_index ]
        
    return taskId, wms, ret_map


class SchedulerGLite (SchedulerInterface) :
    """
    basic class to handle glite jobs
    """
    def __init__( self, **args):


        # call super class init method
        super(SchedulerGLite, self).__init__(**args)

    delegationId = ""

    ##########################################################################

    def delegateProxy( self, wms ):
        """
        delegate proxy to a wms
        """
        
        msg = self.ExecuteCommand(
            "export X509_USER_PROXY=" + self.cert \
            + "; glite-wms-job-delegate-proxy -d " +  self.delegationId \
            + " --endpoint " + wms
            )

        if msg.find("Error -") >= 0 :
            print "Warning : \n", msg


    ##########################################################################
    def submit( self, obj, requirements='', config ='', service='' ):
        """
        submit a jdl to glite
        ends with a call to retrieve wms and job,gridid asssociation
        """

        # decode object
        jdl = self.decode( obj, requirements )
        
        # write a jdl tmpFile
        tmp, fname = tempfile.mkstemp( "", "glite_bulk_", os.getcwd() )
        tmpFile = open( fname, 'w')
        tmpFile.write( jdl )
        tmpFile.close()
        command = "export X509_USER_PROXY=" + self.cert + '; '
        
        # delegate proxy
        if self.delegationId != "" :
            command += "glite-wms-job-submit -d " + self.delegationId
            self.delegateProxy( service )
        else :
            command += "glite-wms-job-submit -a "
        
        if len(config) != 0 :
            command += " -c " + config

        if service != '' :
            command += ' -e ' + service

        out = self.ExecuteCommand(
            command + ' ' + fname, userProxy = self.cert
            )
        try:
            c = out.split("Your job identifier is:")[1].strip()
            taskId = c.split("=")[0].strip()
        except IndexError:
            raise SchedulerError( 'wrong parent id',  out )
        
        print "Your job identifier is: ", taskId
        return getChildrens( taskId )


    ##########################################################################

    def getOutput( self, obj, outdir='' ):
        """
        retrieve job output
        """

        schedIdList = []

        # the object passed is a job
        if type(obj) == Job and self.valid( obj.runningJob ):

            # check for the RunningJob integrity
            schedIdList = [ str( obj.runningJob['schedulerId'] ).strip() ]

        # the object passed is a Task
        elif type(obj) == Task :

            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                schedIdList.append(
                    str( job.runningJob['schedulerId'] ).strip() )

        for jobId in schedIdList: 
            command = "export X509_USER_PROXY=" + self.cert \
                      + "; glite-wms-job-output --noint --dir " \
                      + outdir + " " + jobId
            out = self.ExecuteCommand( command )
            if out.find("have been successfully retrieved") == -1 :
                raise SchedulerError( 'retrieved', out )


    ##########################################################################

    def kill( self, obj ):
        """
        kill job
        """

        # the object passed is a job
        if type(obj) == Job and self.valid( obj.runningJob ):

            # check for the RunningJob integrity
            schedIdList = str( obj.runningJob['schedulerId'] ).strip()

        # the object passed is a Task
        elif type(obj) == Task :

            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                schedIdList += " " + \
                               str( job.runningJob['schedulerId'] ).strip()

        command = "export X509_USER_PROXY=" + self.cert \
                  + "; glite-wms-job-cancel --noint " + schedIdList
        out = self.ExecuteCommand( command )
        if out.find("glite-wms-job-cancel Success") == -1 :
            raise SchedulerError( 'error', out )

    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        execute a resources discovery through wms
        """

        # write a jdl file
        tmp, fname = tempfile.mkstemp( "", "glite_list_match_", os.getcwd() )
        tmpFile = open( fname, 'w')
        jdl = self.decode( obj, requirements='' )
        tmpFile.write( jdl )
        tmpFile.close()
        command = "export X509_USER_PROXY=" + self.cert + '; '
    
        # delegate proxy
        if self.delegationId == "" :
            command = "glite-wms-job-list-match -d " + self.delegationId
            self.delegateProxy( service )
        else :
            command = "glite-wms-job-list-match -a "
        
        if len(config) != 0 :
            command += " -c " + config

        if service != '' :
            command += ' -e ' + service


        out = self.ExecuteCommand(
            command + ' ' + fname, userProxy = self.cert
            )
        try:
            out = out.split("CEId")[1].strip()
        except IndexError:
            raise ( 'IndexError', out )

        return out.split()
        

    ##########################################################################

    def postMortem( self, schedulerId, outfile, service):
        """
        perform scheduler logging-info
        
        """

        command = "glite-wms-job-logging-info -v 2 " + schedulerId + \
                  " > " + outfile + "/gliteLoggingInfo.log"

        return self.ExecuteCommand( command, userProxy = self.cert )


    ##########################################################################

    def query(self, schedIdList, service='', objType='node') :

        """
        query status and eventually other scheduler related information
        """
        if type == 'node':
            return checkJobs( schedIdList )
        elif type == 'parent' :
            return checkJobsBulk( schedIdList )

    ##########################################################################

    def jobDescription ( self, obj, requirements='', config='', service = '' ):

        """
        retrieve scheduler specific job description
        """

        # decode obj
        return self.decode( obj, requirements='' )

        
    ##########################################################################

    def decode  ( self, obj, requirements='' ) :

        if type(obj) == RunningJob or type(obj) == Job :
            return self.jdlFile ( obj, requirements ) 
        elif type(obj) == Task :
            return self.collectionJdlFile ( obj, requirements ) 


    ##########################################################################

    def fullPath( self, path, startdir ) :
        """
        should be in task/job to resolve input/output fullpaths
        """
        if path == '' :
            return path
        if startdir != '' :
            return os.path.normpath( os.path.join(startdir, path) )
        else :
            return os.path.normpath( os.path.join(os.getcwd(), path) )

    ##########################################################################

    def jdlFile( self, job, requirements='' ) :
        """
        build a job jdl
        """

        # general part
        jdl = "[\n"
        jdl += 'Type = "job";\n'
        jdl += 'Executable = "%s";\n' % job[ 'executable' ]
        jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
        if job[ 'standardInput' ] != '':
            jdl += 'StdInput = "%s";\n' % job[ 'standardInput' ]
        jdl += 'StdOutput  = "%s";\n' % job[ 'standardOutput' ]
        jdl += 'StdError   = "%s";\n' % job[ 'standardError' ]

        # input files handling
        infiles = ''
        for infile in job['fullPathInputFiles'] :
            if infile != '' :
                infiles += '"file://' + infile + '",'
        if len( infiles ) != 0 :
            jdl += 'InputSandbox = {%s};\n'% infiles[:-1]

        # job output files handling
        outfiles = ''
        for outfile in job['fullPathOutputFiles'] :
            if outfile != '' :
                outfiles += '"' + outfile + '",'
        if len( outfiles ) != 0 :
            jdl += 'OutputSandbox = {%s};\n'% outfiles[:-1]
 
        # extra job attributes
        if job.runningJob is not None \
               and job.runningJob[ 'schedulerAttributes' ] is not None :
            jdl += job.runningJob[ 'schedulerAttributes' ]

        # blindly append user requirements
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            jdl += '\n' + requirements + '\n'
        except :
            pass

        # return value
        return jdl

    ##########################################################################

    def collectionJdlFile( self, task, requirements='' ):
        """
        build a collection jdl
        """
        
        # general part for task
        jdl = "[\n"
        jdl += "Type = \"collection\";\n"
        for key, val in task.attr_list.iteritems() :
            jdl += "%s = %s;\n" % ( key, val )
        infiles = ''
        for files in task.sandbox :
            if files == '' :
                continue
            infiles += '"file://' + task.fullPath( files ) + '",'
        jdl += 'InputSandbox = {"%s"};\n' % infiles[:-1]

        # single job definition
        jdl += "Nodes = {\n"
        for job in task.getJobs() :
            jdl += "[\n"
            jdl += 'NodeName   = "%s";\n' % job[ 'name' ]
            jdl += 'Executable = "%s";\n' % job[ 'executable' ]
            jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
            if job.attr[ 'standardInput' ] != '':
                jdl += 'StdInput  = "%s";\n' % job[ 'standardInput' ]
            jdl += 'StdOutput  = "%s";\n' % job[ 'standardOutput' ]
            jdl += 'StdError   = "%s";\n' % job[ 'standardError' ]
            for key, val in job.attr_list.iteritems() :
                jdl += '%s  = %s;\n' % ( key, val )

            # job input files handling
            infiles = ''
            for infile in job['fullPathInputFiles'] :
                if infile != '' :
                    infiles += '"file://' + infile + '",'
            if len( infiles ) != 0 :
                jdl += 'InputSandbox = {%s};\n'% infiles[:-1]

            # job output files handling
            outfiles = ''
            for outfile in job['fullPathOutputFiles'] :
                if outfile != '' :
                    outfiles += '"' + outfile + '",'

            # global sandbox definition
            if len( outfiles ) != 0 :
                jdl += 'OutputSandbox = {%s};\n'% outfiles[:-1]
            jdl += "],\n"
        jdl = jdl[:-2] +"\n};\n"


        # extra job attributes
        if job.runningJob is not None \
               and job.runningJob[ 'schedulerAttributes' ] is not None :
            jdl += job.runningJob[ 'schedulerAttributes' ]

        # blindly append user requirements
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            jdl += '\n' + requirements + '\n'
        except :
            pass
        
        # return value
        return jdl


