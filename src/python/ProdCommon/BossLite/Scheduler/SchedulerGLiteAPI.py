#!/usr/bin/env python
"""
_SchedulerGLiteAPI_
"""

__revision__ = "$Id: SchedulerGLiteAPI.py,v 1.42 2008/04/29 08:15:42 gcodispo Exp $"
__version__ = "$Revision: 1.42 $"

import sys
import os
import traceback
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

#
# Import gLite specific modules
try:
    from wmproxymethods import Wmproxy
    from wmproxymethods import BaseException
except StandardError, e:
    warn = \
         """
         missing glite environment.
         Try export PYTHONPATH=$PYTHONPATH:$GLITE_LOCATION/lib
         """
    raise ImportError(warn + str(e))


##########################################################################

def processRow ( row ):
    """
    Utility fuction
    
    Process jdl line, smart comment handling
    """
    row = row.strip()
    if len( row ) == 0 :
        raise StandardError
    if row[0] == '#' :
        try:
            row = row[ row.find('\n') : ].strip()
            return processRow ( row )
        except StandardError, err:
            raise err
    index = row.find('=')
    key = row[0:index].strip().lower()
    val = row[index+1:].strip()
    if len(key) == 0 :
        raise StandardError
    return key, val


def processClassAd(  ClassAd ):
    """
    Utility fuction
    
    extract entries from a jdl
    """
    endpoints = []
    cladDict = {}
    configfile = ""
    try:
        if len(ClassAd) == 0 :
            raise SchedulerError( "bad jdl ", "empty ClassAd" )
        while ClassAd[0] == '[':
            ClassAd = ClassAd[1:-1].strip()
        if ClassAd.find("WmsClient") >= 0 :
            ClassAd = (ClassAd.split("WmsClient")[1]).strip()
            while ClassAd[0] == '[' or ClassAd[0] == '=' :
                ClassAd = ClassAd[1:-1].strip()
        cladMap = ClassAd.split(';')
        for p in cladMap:
            p = p.strip()
            try:
                key, val = processRow ( p )
            except StandardError:
                continue
            if ( key == "wmsconfig" ) :
                configfile = val.replace("\"", "")
            elif ( key == "wmproxyendpoints" ) :
                wms = val[ val.find('{') +1 : val.find('}') ]
                wms = wms.replace("\n", " ")
                wms = wms.replace("#", ",#")
                endpoints = endpoints + wms.split(',')
            else :
                cladDict[ key ] = val
    except StandardError:
        raise SchedulerError( "bad jdl ", traceback.format_exc() )
    
    return cladDict, endpoints, configfile


def parseConfig ( configfile, vo='cms' ):
    """
    Utility fuction
    
    extract entries from glite config files
    """

    cladAddDict = {}
    endpoints = []
 #   print "using config file", configfile
    try:
        if ( len(configfile) == 0 ):
            configfile = "%s/etc/%s/glite_wms.conf" \
                         % ( os.environ['GLITE_LOCATION'], vo )

        fileh = open( configfile, "r" )
        configClad = fileh.read().strip()
        cladAddDict, endpoints, dummyfile = processClassAd( configClad )
    except  StandardError, err:
        print "Warning : \n" + err.__str__()

    if ( len(endpoints) == 0  ) :
        raise SchedulerError( "bad jdl ", "No WMS defined" )

    cladadd = ''
    for k, v in cladAddDict.iteritems():
        cladadd += k + ' = ' + v + ';\n'

    return endpoints, cladadd


    ##########################################################################
class SchedulerGLiteAPI(SchedulerInterface) :
    """
    basic class to handle glite jobs through wmproxy API
    """
    def __init__( self, **args ):

        # call super class init method
        super(SchedulerGLiteAPI, self).__init__(**args)

    delegationId = "bossproxy"
    SandboxDir = "SandboxDir"
    zippedISB  = "zippedISB.tar.gz"
        
    def mergeJDL( self, jdl, wms='', configfile='' ):
        """
        parse config files, merge jdl and retrieve wms list
        """
        
        try:
            schedClassad = ""
            endpoints = []
            if len( wms ) == 0:
                pass
            elif type( wms ) == str :
                endpoints = [ wms ]
            elif type( wms ) == list :
                endpoints = wms
                
            if len( endpoints ) == 0 :
                endpoints, schedClassad = parseConfig ( configfile )
            else :
                tmp, schedClassad = parseConfig ( configfile )
                
            begin = ''
            jdl.strip()
            if jdl[0] == '[' :
                begin = '[\n'
                jdl = begin + schedClassad + jdl[1:]
        except SchedulerError, err:
            raise err
        except StandardError:
            error = str ( traceback.format_exc() )
            raise SchedulerError( "failed submission", error )

        return jdl, endpoints

    ##########################################################################

    def wmproxySubmit( self, jdl, wms, sandboxFileList ) :
        """
        actual submission function
        
        provides the interaction with the wmproxy.
        needs some cleaning
        """

        ret_map = {}
        taskId = ''
    
        try :
            # first check if the sandbox dir can be created
            if os.path.exists( self.SandboxDir ) != 0:
                raise SchedulerError( "existing " + self.SandboxDir, error )

            # initialize wms connection
            wmproxy = Wmproxy(wms, proxy=self.cert)
            wmproxy.soapInit()

            # delegate proxy
            self.delegateProxy( wms )

            # register job: time consumng operation
            task = wmproxy.jobRegister ( jdl, self.delegationId )

            # retrieve parent id
            taskId = str( task.getJobId() )
            
            # retrieve nodes id
            dag = task.getChildren()
            for job in dag:
                ret_map[ str( job.getNodeName() ) ] = str( job.getJobId() )

            # handle input sandbox :
            if sandboxFileList != '' :
                #   get destination
                destURI = wmproxy.getSandboxDestURI(taskId)

                #   make directory struct locally
                basedir = self.SandboxDir + \
                          destURI[0].split('/' + self.SandboxDir)[1]
                os.makedirs( basedir )

                # copy files in the directory
                msg = \
                    self.ExecuteCommand(
                    "cp %s %s" % (sandboxFileList, basedir)
                    )
                if msg != '' :
                    raise SchedulerError( "cp error", msg )

                # zip sandbox + chmod workarond for the wms
                msg = self.ExecuteCommand(
                    "chmod 773 " + self.SandboxDir +"; chmod 773 " \
                    + self.SandboxDir + "/*"
                    )
                msg = self.ExecuteCommand(
                    "tar pczf %s %s/*"%(self.zippedISB, basedir))
                if msg != '' :
                    raise SchedulerError( "tar error", msg )

                # copy file to the wms (also usable curl)
                #
                # command = "/usr/bin/curl --cert  " + self.cert + \
                #          " --key " + self.cert + \
                #          " --capath /etc/grid-security/certificates " + \
                #          " --upload-file://%s/%s %s/%s "

                command = "globus-url-copy file://%s/%s %s/%s" \
                          % ( os.getcwd(), self.zippedISB, \
                              destURI[0], self.zippedISB )
                msg = self.ExecuteCommand(command)
                if msg.upper().find("ERROR") >= 0 \
                       or msg.find("wrong format") >= 0 :
                    raise SchedulerError( "globus-url-copy error", msg )

            # start job!
            wmproxy.jobStart(taskId)
            
            # cleaning up everything: delete temporary files and exit
            if sandboxFileList != '' :
                msg = self.ExecuteCommand( "rm -rf " + self.SandboxDir \
                                           + ' ' + self.zippedISB )
                if msg != '' :
                    print "Warning : " + msg

        except BaseException, err:
            os.system( "rm -rf " + self.SandboxDir + ' ' + self.zippedISB )
            raise SchedulerError( "failed submission to " + wms, err.toString() )
        except SchedulerError, err:
            os.system( "rm -rf " + self.SandboxDir + ' ' + self.zippedISB )
            SchedulerError( "failed submission to " + wms, err )
        except StandardError:
            os.system( "rm -rf " + self.SandboxDir + ' ' + self.zippedISB )
            error = str ( traceback.format_exception(sys.exc_info()[0],
                                                     sys.exc_info()[1],
                                                     sys.exc_info()[2]) )
            raise SchedulerError( "failed submission to " + wms, error )

                
        return taskId, ret_map

    ##########################################################################

    def delegateProxy( self, wms ):
        """
        delegate proxy to a wms
        """

        command = "glite-wms-job-delegate-proxy -d " +  self.delegationId \
                  + " --endpoint " + wms
        
        if self.cert != '' :
            command = "export X509_USER_PROXY=" + self.cert + ' ; ' + command

        msg = self.ExecuteCommand( command )

        if msg.find("Error -") >= 0 :
            print "Warning : \n", msg

    ##########################################################################

    def submit( self, obj, requirements='', config ='', service='' ):
        """
        user submission function
        
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

        wms = service
        configfile = config
        # decode obj
        jdl, sandboxFileList = self.decode( obj, requirements )

        # return values
        taskId = ''
        ret_map = {}

        # handle wms
        jdl, endpoints = self.mergeJDL( jdl, wms, configfile )

        # jdl ready!
        # print "Using jdl : \n" + jdl

        # installing a signal handler to clean files if the submission
        # is signaled e.g. for a timeout
        # signal.signal(signal.SIGTERM, handler)

        # emulate ui round robin
        try :
            import random
            random.shuffle(endpoints)
        except:
            print "random access to wms not allowed, using sequential access"

        errors = ''
        success = None
        seen = []
        for wms in endpoints :
            try :
                wms = wms.replace("\"", "").strip()
                if  len( wms ) == 0 or wms[0]=='#' or wms in seen:
                    continue
                else :
                    seen.append( wms)
                print "Submitting to : " + wms
                taskId, ret_map = \
                        self.wmproxySubmit( jdl, wms, sandboxFileList )
                success = wms
                break
            except SchedulerError, err:
                errors += str( err )
                continue

        # clean files
        os.system("rm -rf " +  self.SandboxDir + ' ' + self.zippedISB)
        # if submission failed, raise error
        if success is None :
            raise SchedulerError( "failed submission", errors )

        return ret_map, taskId, success


    ##########################################################################

    def getOutput( self, obj, outdir='', service='' ):
        """
        retrieve output or just put it in the destination directory

        """

        # the object passed is a job
        if type(obj) == Job :

            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # retrieve output
            self.getWMSOutput( [ obj ], outdir, obj.runningJob['service']  )

            # errors?
            if obj.runningJob.isError() :
                raise SchedulerError( str( obj.runningJob.errors[0][0], \
                                           obj.runningJob.errors[0][1] ))

        # the object passed is a Task
        elif type(obj) == Task :

            if outdir == '' :
                outdir = obj['outputDirectory']

            # retrieve scheduler id list
            schedIdList = {}
            for job in obj.jobs:
                if self.valid( job.runningJob ):
                    if not schedIdList.has_key( job.runningJob['service'] ) :
                        schedIdList[job.runningJob['service']] = []
                    schedIdList[job.runningJob['service']].append( job )

            # retrieve output for all jobs
            for service, idList in schedIdList.iteritems() :
                self.getWMSOutput( idList, outdir, service )

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))

        
    ##########################################################################

    def getWMSOutput( self, jobList,  outdir, service ):
        """
        Manage objects to retrieve the output
        """

        # skip empty endpoint
        wms = service.strip()
        if len(wms) == 0 :
            return

        # look for a well formed name
        if wms.find( 'https') < 0 :
            wms = 'https://' + wms + ':7443/glite_wms_wmproxy_server'

        # initialize wmproxy
        wmproxy = Wmproxy(wms, proxy=self.cert)
        wmproxy.soapInit()

        # loop ove jobs
        for job in jobList:

            # skip malformed id
            jobId = str( job.runningJob['schedulerId'] ).strip()
            if jobId is None or len(jobId) == 0 :
                continue

            # eventual error container
            joberr = ''

            # get file list
            try :
                filelist = wmproxy.getOutputFileList( jobId )
            except BaseException, err:
                output = err.toString()

                # proxy expired: skip!
                if output.find( 'Error with credential' ) != -1 :
                    job.runningJob.errors.append( [ 'Error with credential', \
                                                    output ] )
                    continue

                # purged: probably already retrieved. Archive
                elif output.find( "has been purged" ) :
                    job.runningJob['statusHistory'].append( output )
                    job.runningJob['statusHistory'].append(
                        'Job has been purged, recovering status' )
                    continue

                # not ready for GO: waiting for next round
                elif output.find( "Job current status doesn" ) != -1 :
                    job.runningJob.errors.append(
                        [ "output not available", output ] )
                    continue

            # retrieve files
            retrieved = 0
            for m in filelist:

                # ugly error: nothing there!
                try :
                    size = int( m['size'] )
                    # ugly trick for empty fields...
                    if m['name'].strip() == '' :
                        raise ValueError
                except ValueError:
                    continue

                # avoid globus-url-copy for empty files
                if size == 0 :
                    joberr +=  'file ' + os.path.basename( m['name'] ) \
                             + ' has zero size;'
                    continue

                # retrieve file
                dest = outdir + '/' + os.path.basename( m['name'] )
                command = "globus-url-copy -verbose " + m['name'] \
                          + " file://" + dest
                msg = self.ExecuteCommand(command)
                if msg.upper().find("ERROR") >= 0 or \
                       msg.find("wrong format") >= 0 :
                    joberr = msg + '; '
                    continue

                # check file size
                if os.path.getsize(dest) !=  size  :
                    joberr =  'size mismatch : expected ' \
                             + str( os.path.getsize(dest) ) \
                             + ' got ' + m['size'] + '; '
                    continue

                # update files counter
                retrieved += 1

            # no files?
            if retrieved == 0:
                job.runningJob['statusHistory'].append(
                    'Warning: non files retrieved' )
                job.runningJob.errors.append( ['Missing Files' , \
                                    'Warning: non files retrieved'] )

            # got errors?
            if joberr != '' :
                job.runningJob.errors.append(
                    ['problems with output retrieval', joberr] )
            else :
                # try to purge files
                try :
                    wmproxy.jobPurge( jobId )
                except BaseException, err:
                    job.runningJob['statusHistory'].append(
                        "WARNING : " + err.toString())
                

    ##########################################################################

    def kill( self, schedIdList, service):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        # skip empty endpoint
        wms = service.strip()
        if len(wms) == 0 :
            return

        # prepare the list of possible exceptions
        errors = {}

        # look for a well formed name
        if wms.find( 'https') < 0 :
            wms = 'https://' + wms + ':7443/glite_wms_wmproxy_server'

        # initialize wmproxy
        wmproxy = Wmproxy(wms, self.cert)
        wmproxy.soapInit()

        # loop ove jobs
        for jobid in schedIdList:

            # skip malformed id
            jobid = str( jobid ).strip()
            if jobid is None or len(jobid) == 0 :
                continue

            try :
                wmproxy.jobCancel( jobid )
            except BaseException, err:
                errors[ jobid ] = err.toString()
                continue
            try :
                wmproxy.jobPurge( jobid )
            except BaseException, err:
                #print "WARNING : " + err.toString()
                continue

        # raise exception for failed operations
        if len( errors ) != 0 :
            raise SchedulerError(
                'scheduler interaction failed for some jobs ', str(errors)
                )

    ##########################################################################

    def killCheck( self, schedIdList, id_type = 'node' ):
        """
        Kill jobs querying WMS to LB, if the job status allows
        If a list of parent id is used, must be id_type='parent'
        """

        jobs = []
        if len( schedIdList ) == 0:
            return
        elif type( schedIdList ) == str :
            jobs = [ schedIdList ]
        elif type( schedIdList ) == list :
            jobs = schedIdList
            
        # retrieve wms
        from ProdCommon.BossLite.Scheduler.GLiteLBQuery import groupByWMS
        endpoints = groupByWMS(
            jobs, self.cert, id_type, \
            status_list = ['Done', 'Aborted','Cancelled'],\
            allow = False
            )
        
        # actual kill
        for wms, schedIdList in endpoints.iteritems():
            try :
                self.kill( wms, schedIdList )
            except StandardError:
                for jobid in schedIdList:
                    print jobid, "failed"

    ##########################################################################

    def purgeService( self, schedIdList ):
        """
        Purge job (even bulk) from wms
        """

        jobs = []
        if len( schedIdList ) == 0:
            return
        elif type( schedIdList ) == str :
            jobs = [ schedIdList ]
        elif type( schedIdList ) == list :
            jobs = schedIdList
        
        # retrieve wms and get output
        from ProdCommon.BossLite.Scheduler.GLiteLBQuery import groupByWMS
        endpoints = groupByWMS(
            jobs, self.cert, 'node', status_list=['Done'], allow=True
            )
        for wms, schedIdList in endpoints.iteritems():
            try:
                wmproxy = Wmproxy( wms, self.cert )
                wmproxy.soapInit()
                for jobid in schedIdList:
                    wmproxy.jobPurge( jobid )
            except BaseException, err:
                print err.toString(), '\n\n'
                print "error"

    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        resources list match
        """
        jdl = self.decode( obj, requirements)
        configfile = config
        wms = service
        matchingCEs = []

        jdl, endpoints = self.mergeJDL( jdl, wms, configfile)

        # jdl ready!
        seen = []

        # emulate ui round robin
        try :
            import random
            random.shuffle(endpoints)
        except:
            print "random access to wms not allowed, using sequential access"

        for wms in endpoints :
            try :
                wms = wms.replace("\"", "").strip()
                if  len( wms ) == 0 or wms[0]=='#' or wms in seen:
                    continue
                else :
                    seen.append( wms)
                print "ListMatch to : ", wms
                
                # delegate proxy
                self.delegateProxy( wms )
                
                # initialize wms connection
                wmproxy = Wmproxy(wms, self.cert)
                wmproxy.soapInit()

                # list match
                matchingCEs = wmproxy.jobListMatch(jdl, "bossproxy")
                if matchingCEs != None and len ( matchingCEs ) != 0 :
                    break
                else :
                    print  "No results for listMatch\n\n" 
            except BaseException, err:
                continue
            except SchedulerError, err:
                continue

        return matchingCEs

    ##########################################################################

    def postMortem( self, schedulerId, outfile, service):
        """
        perform scheduler logging-info
        
        """
        command = "glite-wms-job-logging-info -v 3 " + schedulerId + \
                  " > " + outfile + ".LoggingInfo"
            
        if self.cert != '' :
            command = "export X509_USER_PROXY=" + self.cert + ' ; ' + command

        return self.ExecuteCommand( command )


    ##########################################################################

    def query(self, schedIdList, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        """
        
        from ProdCommon.BossLite.Scheduler.GLiteLBQuery import \
             checkJobs, checkJobsBulk
        if objType == 'node':
            return checkJobs( schedIdList, self.cert )
        elif objType == 'parent' :
            return checkJobsBulk( schedIdList, self.cert )


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
        return self.mergeJDL( jdl, service, config )[0]


    ##########################################################################
    def decode  ( self, obj, requirements='' ) :
        """
        prepare file for submission
        """
        if type(obj) == Job :
            return self.singleApiJdl ( obj, requirements )
        elif type(obj) == Task :
            return self.collectionApiJdl ( obj, requirements )
            #if len( obj.jobs ) == 1:
            #    return self.singleApiJdl ( obj.jobs[0], requirements )
            #else:
            #    return self.collectionApiJdl ( obj, requirements )


    ##########################################################################

    def singleApiJdl( self, job, requirements='' ) :
        """
        build a job jdl easy to be handled by the wmproxy API interface
        and gives back the list of input files for a better handling
        """

        # general part
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
        remOutfiles = ''
        for filePath in job['outputFiles'] :
            if filePath.find('gsiftp://') >= 0 :
                remOutfiles += '"' + filePath + '",'
            elif filePath != '' :
                outfiles += '"' + filePath + '",'
        if len( outfiles ) != 0 :
            jdl += 'OutputSandbox = {%s};\n' % outfiles[:-1]
        if len( remOutfiles ) != 0 :
            jdl += 'OutputSandboxDestURI = {%s};\n' % remOutfiles[:-1]

        # extra job attributes
        if job.runningJob is not None \
               and job.runningJob[ 'schedulerAttributes' ] is not None :
            jdl += job.runningJob[ 'schedulerAttributes' ]

        # blindly append user requirements
        jdl += requirements + '\n]\n'
        
        # return values
        #print jdl
        return jdl, filelist

    ##########################################################################

    def collectionApiJdl( self, task, requirements='' ):
        """
        build a collection jdl easy to be handled by the wmproxy API interface
        and gives back the list of input files for a better handling
        """
        
        # general part for task
        jdl = "[\n"
        jdl += 'Type = "collection";\n'

        # global task attributes :
        # \\ the list of files for the JDL common part
        GlobalSandbox = ''
        # \\ the list of physical files to be returned
        filelist = ''
        # \\ the list of common files to be put in every single node
        #  \\ in the form root.inputsandbox[ISBindex]
        commonFiles = ''
        ISBindex = 0

        # task input files handling:
        if task['startDirectory'] is None or task['startDirectory'][0] == '/':
            # files are stored locally, compose with 'file://'
            if task['globalSandbox'] is not None :
                for ifile in task['globalSandbox'].split(','):
                   # print files
                    if ifile == '' :
                        continue
                    filename = os.path.abspath( ifile )
                    GlobalSandbox += '"file://' + filename + '",'
                    filelist += filename + ' '
                    commonFiles += "root.inputsandbox[%d]," % ISBindex
                    ISBindex += 1
        else :            
            # files are elsewhere, just add their composed path
            if task['globalSandbox'] is not None :
                jdl += 'InputSandboxBaseURI = "%s";\n' % task['startDirectory']
                for ifile in task['globalSandbox'].split(','):
                    if ifile == '' :
                        continue
                    #filename = task['startDirectory'] + '/' + ifile
                    #GlobalSandbox += '"' + filename + '",'
                    if ifile[0] == '/':
                        ifile = ifile[1:]
                    #GlobalSandbox += '"' + ifile + '",'
                    #commonFiles += "root.inputsandbox[%d]," % ISBindex
                    commonFiles += '"' + ifile + '",'
                    ISBindex += 1

        # single job definition
        jdl += "Nodes = {\n"
        for job in task.jobs :
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
            remOutfiles = ''
            for filePath in job['outputFiles'] :
                if filePath.find('gsiftp://') >= 0 :
                    remOutfiles += '"' + filePath + '",'
                elif filePath != '' :
                    outfiles += '"' + filePath + '",'
            if len( outfiles ) != 0 :
                jdl += 'OutputSandbox = {%s};\n' % outfiles[:-1]
            if len( remOutfiles ) != 0 :
                jdl += 'OutputSandboxDestURI = {%s};\n' % remOutfiles[:-1]

            # job input files handling:
            # add their name in the global sanbox and put a reference
            localfiles = commonFiles
            if task['startDirectory'] is None \
                   or task['startDirectory'][0] == '/':
                # files are stored locally, compose with 'file://'
                for filePath in job['fullPathInputFiles']:
                    if filePath != '' :
                        localfiles += "root.inputsandbox[%d]," % ISBindex
                    GlobalSandbox += '"file://' + filePath + '",'
                    filelist += filePath + ' '
                    ISBindex += 1
            else :
                # files are elsewhere, just add their composed path
                for filePath in job['fullPathInputFiles']:
                    if filePath[0] == '/':
                        filePath = filePath[1:]
                    localfiles += '"' + filePath + '",'

            if localfiles != '' :
                jdl += 'InputSandbox = {%s};\n'% localfiles[:-1]
            jdl += '],\n'
        jdl  = jdl[:-2] + "\n};\n"
        
        # global sandbox definition
        if GlobalSandbox != '' :
            jdl += "InputSandbox = {%s};\n"% (GlobalSandbox[:-1])

        # blindly append user requirements
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            jdl += '\n' + requirements + '\n'
        except :
            pass

        # useful attributes if the transfer is not direct to the WN
        if filelist != '':
            jdl += 'AllowZippedISB = true;\n'
            jdl += 'ZippedISB = "%s";\n' % self.zippedISB

        # close jdl
        jdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        jdl += "\n]\n"

        # return values
        # print jdl, filelist
        return jdl, filelist


    ##########################################################################
    def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):        
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """
        
        celist = []

        if seList == ['']: seList = None
        if blacklist == ['']: blacklist = None
        if whitelist == ['']: whitelist = None

        if blacklist is None :
            blacklist = []

        if len( tags ) != 0 :
            query =  ','.join(  ["Tag=%s" % tag for tag in tags ] ) + \
                    ',CEStatus=Production'
        else :
            query = 'CEStatus=Production'

        command = "export X509_USER_PROXY=" + self.cert + '; '

        if seList == None :
            command += " lcg-info --vo " + vo + " --list-ce --query " + \
                       "\'" + query + "\' --sed"
            out = self.ExecuteCommand( command )
            out = out.split()
            for ce in out :
                # blacklist
                if ce.find( "blah" ) == -1:
                    passblack = 1
                    if len(blacklist) > 0:
                        for ceb in blacklist :
                            if ce.find(ceb) > 0:
                                passblack = 0
                # whitelist if surviving the blacklist selection
                if passblack:
                    if whitelist is None or len(whitelist)==0 :
                        celist.append( ce )
                    else:
                        for cew in whitelist:
                            if ce.find(cew)>=0:
                                celist.append( ce )

            return celist

        for se in seList :
            singleComm = command + " lcg-info --vo " + vo + \
                         " --list-ce --query " + \
                         "\'" + query + ",CloseSE="+ se + "\' --sed"

            out = self.ExecuteCommand( singleComm )
            out = out.split()
            for ce in out :
                # blacklist
                if ce.find( "blah" ) == -1:
                    passblack = 1
                    if len(blacklist) > 0:
                        for ceb in blacklist :
                            if ce.find(ceb) > 0:
                                passblack = 0
                # whitelist if surviving the blacklist selection
                if passblack:
                    if whitelist is None or len(whitelist)==0 :
                        celist.append( ce )
                    else:
                        for cew in whitelist:
                            if ce.find(cew)>=0:
                                celist.append( ce )

        return celist
 

