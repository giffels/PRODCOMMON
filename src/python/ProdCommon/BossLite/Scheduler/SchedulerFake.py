#!/usr/bin/env python
"""
_SchedulerFake_
"""

__revision__ = "$Id: SchedulerFake.py,v 1.3 2008/03/27 14:51:30 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"

import traceback
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
#
# Import scheduler specific modules
#
# Add any generic utility function
#


    ##########################################################################
class SchedulerFake(SchedulerInterface) :
    """
    basic class to scheduler usage
    """
    def __init__( self, user_proxy = '' ):

        # call super class init method
        super(SchedulerFake, self).__init__(user_proxy)

    # Generic static parameter, if needed
    delegationId = "bossproxy"
    SandboxDir = "SandboxDir"
    zippedISB  = "zippedISB.tar.gz"
    names = {}

    ##########################################################################
    def mergeConfig( self, jdl, configfile='' ):
        """
        maybe we need to merge a configuration file with the job description
        """
        
        jdl = configfile + jdl

        return jdl


    ##########################################################################

    def actualSubmit( self, jdl, service, options = '' ) :
        """
        actual submission function
        
        provides the interaction with the scheduler
        """

        ret_map = {}
        taskId = ''
    
        try :
            # connect to service if needed
            # e.g. : wmproxy = Wmproxy(wms, proxy=self.cert)

            # submit:
            # 1)
            #   out = self.ExecuteCommand( "qsub " + options + ' ' + jdl  )
            # 2)
            #   task = wmproxy.jobRegister ( jdl, self.delegationId )

            # retrieve parent id
            # 1)
            #   ft = re.compile("Some regexp")
            #   task = ft.match( out ).group(1, 2, ...)
            #   nay regexp to fill up a map { "jobName" : "schedId" }
            # 2)
            #   taskId = str( task.getJobId() )
            #   dag = task.getChildren()
            #   for job in dag:
            #      ret_map[ str( job.getNodeName() ) ] = str( job.getJobId() )

            # fake output:
            taskId = "ImTheParent"
            idx = 0
            for job in self.names:
                ret_map[ job ] = "child_%d" % idx
                idx += 1

            
        except SchedulerError, err:
            SchedulerError( "failed submission to " + service, err )
        except StandardError, error:
            raise SchedulerError( "failed submission to " + service, error )
                
        return taskId, ret_map


    ##########################################################################

    def submit( self, obj, requirements='', config ='', service='' ):
        """
        user submission function
        
        takes as arguments:
        - a finite, dedicated jdl
        - eventually a list of services to connect
        - eventually a config file

        the passed config file or, if not provided, a default one can be
        used from eventual defaults

        the function returns an eventual parent id, the service of the
        successfully submission and a map associating the jobname to the
        node id. If the submission is not bulk, the parent id should be the
        node id of the unique entry of the map
        
        """

        # decode obj
        jdl = self.decode( obj, requirements )

        # return values
        taskId = ''
        ret_map = {}

        # handle config file
        jdl = self.mergeConfig( jdl, config )

        # jdl ready!
        # print "Using jdl : \n" + jdl

        # emulate ui round robin
        if type( service ) == str :
            service = [ service ]
        try :
            import random
            random.shuffle(endpoints)
        except:
            print "random access to wms not allowed, using sequential access"
            pass

        success = ''
        seen = []
        for serv in service :
            try :
                print "Submitting to : " + serv
                # use a specific method???
                taskId, ret_map = \
                        self.actualSubmit( jdl, serv )
                success = serv
                break
            except SchedulerError, err:
                print err
                continue

        # clean eventual files if needed...
        return ret_map, taskId


    ##########################################################################

    def getOutput( self, obj, outdir='', service='' ):
        """
        retrieve output or just put it in the destination directory
        """

        # obj can be a task, a job or even a running job
        # several possibilities:
        # 1) connect to a service and perform a remote copy
        # 2) just eventually copy the local output to the destination dir
        # 3) wrap a CLI command like glite-wms-job-output

        out = "whatever" 
        if out.find( 'error' ) >= 0 :
            raise SchedulerError ( "Unable to retrieve output", out )

    ##########################################################################

    def kill( self, schedIdList, service):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        # several possibilities:
        # 1) connect to a service and perform a kill
        # 2) wrap a CLI command like glite-wms-job-cancel
        
        out = "whatever" 
        if out.find( 'error' ) >= 0 :
            raise SchedulerError ( "Unable to kill job", out )

    ##########################################################################

    def purgeService( self, schedIdList ):
        """
        Purge job (even bulk) from service
        """

        # not always available...
        # it may be useful to connect to a remote service and purge job sandbox
        
        out = "whatever" 
        if out.find( 'error' ) >= 0 :
            raise SchedulerError ( "Unable to purge job", out )


    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        resources list match
        """

        # several possibilities:
        # 1) connect to a service and ask
        # 2) wrap a CLI command like glite-wms-job-listmatch
        # 3) nor available... skip
        # 4) there is a useful lcgInfo...
        
        out = "whatever" 
        if out.find( 'error' ) >= 0 :
            raise SchedulerError ( "Unable to find resources", out )


    ##########################################################################

    def postMortem( self, schedulerId, outfile, service):
        """
        perform scheduler logging-info
        
        """
        # here an actual example 
        
        command = "glite-wms-job-logging-info -v 2 " + schedulerId + \
                  " > " + outfile + "/gliteLoggingInfo.log"
            
        return self.ExecuteCommand( command, userProxy = self.cert )


    ##########################################################################

    def query(self, schedIdList, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        """

        # ask for the job informations, mainly status
        # some systems allow a query job per job, others also bulk queries

        ret_map = {}

        if objType == 'node':
            for schedId in schedIdList :
                # do query
                # ...
                values = { 'destination' : 'home',
                           'statusScheduler' : 'Running',
                           'status' : 'R',
                           'statusReason' : 'on site'}
                ret_map[ schedId ] = values
        elif objType == 'parent' :
            for schedId in schedIdList :
                #  a bulk command  may give many jobs in one shot!
                newIdList = bulkQueryCommand( schedId )
                for newId in newIdList :
                    values = { 'destination' : 'home',
                               'statusScheduler' : 'Running',
                               'status' : 'R',
                               'statusReason' : 'on site'}
                    ret_map[ newId ] = values

        return ret_map


    ##########################################################################

    def jobDescription ( self, obj, requirements='', config='', service = '' ):

        """
        retrieve scheduler specific job description
        """

        # decode obj
        jdl, sandboxFileList = self.decode( obj, requirements )

        # return values
        taskId = ''
        ret_map = {}

        # handle wms
        return self.mergeConfig( jdl, service, config )[0]


    ##########################################################################
    def decode  ( self, obj, requirements='' ) :
        """
        prepare file for submission
        """
        if type(obj) == RunningJob or type(obj) == Job :
            return self.singleApiJdl ( obj, requirements ) 
        elif type(obj) == Task :
            return self.collectionApiJdl ( obj, requirements ) 


    ##########################################################################

    def singleApiJdl( self, job, requirements='' ) :
        """
        build a job jdl easy to be handled by the wmproxy API interface
        and gives back the list of input files for a better handling
        """

        # general part
        self.names = {}
        self.names[job[ 'name' ]] = ''
        jdl = "[\n"
        jdl += 'Type = "job";\n'
        jdl += 'AllowZippedISB = true;\n'
        jdl += 'ZippedISB = "%s";\n'  % self.zippedISB
        jdl += 'Executable = "%s";\n' % job[ 'executable' ]
        jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
        if job[ 'standardInput' ] != '':
            jdl += 'StdInput = "%s";\n' % job[ 'standardInput' ]
        jdl += 'StdOutput  = "%s";\n' % job[ 'standardOutput' ]
        jdl += 'StdError   = "%s";\n' % job[ 'standardError' ]

        # input files handling
        infiles = ''
        filelist = ''
        for infile in job['fullPathInputFiles'] :
            if infile != '' :
                infiles += '"file://' + infile + '",'
            filelist += infile + ' '
        if len( infiles ) != 0 :
            jdl += 'InputSandbox = {%s};\n'% infiles[:-1]

        # output files handling
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
        jdl += requirements + '\n]\n'
        
        # return values
        return jdl

    ##########################################################################

    def collectionApiJdl( self, task, requirements='' ):
        """
        build a collection jdl easy to be handled by the wmproxy API interface
        and gives back the list of input files for a better handling
        """
        
        # general part for task
        jdl = "[\n"
        jdl += 'Type = "collection";\n'
        jdl += 'AllowZippedISB = true;\n'
        jdl += 'ZippedISB = "%s";\n' % self.zippedISB

        # global task attributes :
        # \\ the list of files for the JDL common part
        GlobalSandbox = ''
        # \\ the list of physical files to be returned
        filelist = ''
        # \\ the list of common files to be put in every single node
        #  \\ in the form root.inputsandbox[ISBindex]
        commonFiles = ''
        ISBindex = 0

        # single job definition
        self.names = {}
        jdl += "Nodes = {\n"
        for job in task.jobs :
            self.names[job[ 'name' ]] = ''
            jdl += '[\n'
            jdl += 'NodeName   = "%s";\n' % job[ 'name' ]
            jdl += 'Executable = "%s";\n' % job[ 'executable' ] 
            jdl += 'Arguments  = "%s";\n' % job[ 'arguments' ]
            if job[ 'standardInput' ] != '':
                jdl += 'StdInput = "%s";\n' % job[ 'standardInput' ]
            jdl += 'StdOutput  = "%s";\n' % job[ 'standardOutput' ]
            jdl += 'StdError   = "%s";\n' % job[ 'standardError' ]
            
            # extra job attributes
            if job.runningJob is not None \
                   and job.runningJob[ 'schedulerAttributes' ] is not None :
                jdl += job.runningJob[ 'schedulerAttributes' ]

            # job output files handling
            outfiles = ''
            for filePath in job['fullPathOutputFiles'] :
                jdl += 'OutputSandbox = {%s};\n'% outfiles[:-1]

            # job input files handling:
            # add their name in the global sanbox and put a reference
            if task['startDirectory'] is None \
                   or task['startDirectory'][0] == '/':
                # files are stored locally, compose with 'file://'
                for filePath in job['fullPathInputFiles']:
                    GlobalSandbox += '"file://' + filePath + '",'
            else :
                # files are elsewhere, just add their composed path
                for filePath in job['fullPathInputFiles']:
                    GlobalSandbox += filePath

            jdl += '],\n'
        jdl  = jdl[:-2] + "\n};\n"
        
        # global sandbox definition
        if GlobalSandbox != '' :
            jdl += "InputSandbox = {%s};\n"% (GlobalSandbox[:-1])
        jdl += \
            'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        
        # blindly append user requirements
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            jdl += '\n' + requirements + '\n'
        except :
            pass
        
        jdl += "]"
        
        # return values
        return jdl


