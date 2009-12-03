#!/usr/bin/env python
"""
_SchedulerGLiteAPI_
"""

__revision__ = "$Id: SchedulerGLiteAPI.py,v 1.132 2009/11/09 10:01:09 gcodispo Exp $"
__version__ = "$Revision: 1.132 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import os
import socket
import tempfile
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
#
# Import gLite specific modules

# new UI requires WMPConfig
try:
    from wmproxymethods import WMPConfig
except StandardError, stde:
    pass
#
try:
    from wmproxymethods import Wmproxy
    from wmproxymethods import BaseException
    from wmproxymethods import WMPException
except StandardError, stde:
    warn = \
         """
         missing glite environment.
         Try export PYTHONPATH=$PYTHONPATH:$GLITE_LOCATION/lib
         """
    raise ImportError(warn + str(stde))

# new UI requires WMPConfig
try:
    from wmproxymethods import WMPConfig
except StandardError, stde:
    pass

##########################################################################

def processRow ( row ):
    """
    Utility fuction

    Process jdl line, smart comment handling
    """

    row = row.strip()
    if len( row ) == 0 :
        return None, None
    if row[0] == '#' :
        row = row[ row.find('\n') : ].strip()
        return processRow ( row )

    index = row.find('=')
    key = row[0:index].strip().lower()
    val = row[index+1:].strip()

    if key == '' or val == '' :
        raise ValueError( row )
    return key, val


def processClassAd( classAd ):
    """
    Utility fuction

    extract entries from a jdl
    """

    endpoints = []
    cladDict = {}
    configfile = ""
    
    if classAd.strip() == '' :
        raise SchedulerError( "bad jdl ", "empty classAd" )
    while classAd[0] == '[':
        classAd = classAd[1:-1].strip()
    if classAd.find("WmsClient") >= 0 :
        classAd = (classAd.split("WmsClient")[1]).strip()
        while classAd[0] == '[' or classAd[0] == '=' :
            classAd = classAd[1:-1].strip()

    # location of a subsection
    index = classAd.find( '[' )
    while index > 0 :

        # start of the jdl key
        start = classAd.rfind( ';', 0, index )
    
        # stop of the jdl key
        stop = classAd.rfind( ']' )

        # key from start to index
        key = classAd[ start + 1 : classAd.find( '=', start, index ) ].strip()

        # extract JdlDefaultAttributes
        if key.lower() == "jdldefaultattributes" :
            retCladDict, endpoints, configfile = \
                         processClassAdBlock( classAd[ index : stop + 1 ] )
            cladDict.update( retCladDict ) 

        # value from index to stop
        elif key.lower() != "wmsconfig" :
            cladDict[key] = classAd[ index : stop + 1 ]
                
        # continue parsing of the jdl
        classAd = classAd[ : start ] + classAd[ stop + 1: ]
        index = classAd.find( '[' )

    # return
    retCladDict, endpoints, configfile = processClassAdBlock( classAd.strip() )
    cladDict.update( retCladDict )
    return cladDict, endpoints, configfile


def processClassAdBlock( classAd ):
    """
    Utility fuction

    extract entries from a jdl block
    """

    endpoints = []
    cladDict = {}
    configfile = ""

    try:

        # strip external '[]' pairs
        while classAd[0] == '[':
            classAd = classAd[1:-1].strip()

        # create attributes map and loop for wms detection
        for p in classAd.split(';'):

            p = p.strip()
            try:
                key, val = processRow ( p )
            except ValueError:
                raise SchedulerError( "bad jdl key", p )

            # take wms config file location
            if ( key == "wmsconfig" ) :
                configfile = val.replace("\"", "")

            # extract wms
            elif ( key == "wmproxyendpoints" ) :
                wmsList = val[ val.find('{') +1 : val.find('}') ].replace(
                    ',', '\n')
                for wms in wmsList.split('\n'):
                    wms = wms[ : wms.find('#') ].replace("\"", "").strip()
                    if wms != '' :
                        endpoints.append( wms )             

            # handle not empty pairs
            elif key is not None:
                cladDict[ key ] = val

    except StandardError, e:
        raise SchedulerError( "bad jdl ", str(e) )

    return cladDict, endpoints, configfile


def formatWmpError( wmpError, isNewUi ) :
    """
    format wmproxy BaseException
    """


    if isNewUi :
        error = ''
        if wmpError.errType:
            error += wmpError.errType
        if wmpError.origin:
            error += " raised by " + wmpError.origin
        if wmpError.methodName:
            error += " (method " + wmpError.methodName+") "
        if wmpError.description:
            error += "\n" + wmpError.description

    else:
        error = wmpError.toString() + '\n'

    for key in list( wmpError ):
        if type( key ) == int:
            error += 'Error Number : ' + str( key ) + '\n'
        else :
            error += str( key ) + '\n'

    return error


##########################################################################
class SchedulerGLiteAPI(SchedulerInterface) :
    """
    basic class to handle glite jobs through wmproxy API
    """

    SandboxDir = "SandboxDir"
    zippedISB  = "zippedISB.tar.gz"

    def __init__( self, **args ):

        # call super class init method
        super(SchedulerGLiteAPI, self).__init__(**args)

        # some initializations
        self.proxyString = ''
        self.envProxy = os.environ.get("X509_USER_PROXY",'')
        self.warnings = []

        # skipWMSAuth
        self.skipWMSAuth = args.get("skipWMSAuth", 0)
        self.skipWMSAuth = int( self.skipWMSAuth )

        # typical options
        self.vo = args.get( "vo", "cms" )
        self.service = args.get( "service", "" )
        self.config = args.get( "config", "" )
        self.delegationId = args.get( "proxyname", "bossproxy" )

        # rename output files with submission number
        self.renameOutputFiles = args.get( "renameOutputFiles", 0 )
        self.renameOutputFiles = int( self.renameOutputFiles )
        # x509 string for cli commands
        self.proxyString = ''
        if self.cert != '':
            self.proxyString = "export X509_USER_PROXY=" + self.cert + ' ; '

        # version check for new features
        version, ret = self.ExecuteCommand( 'glite-version' )
        version = version.strip()
        self.isNewUi = ( version.find( '3.2' ) == 0 )

        ### # check which UI version we are in
        ### globusloc = os.environ['GLOBUS_LOCATION']
        ### globusv = re.compile('.*3.1.(\d*).*')
        ### if globusv.search(globusloc):
        ###     vers = int(globusv.search(globusloc).groups()[0])
        ###     if vers < 10:
        ###         self.newUI = False


    ##########################################################################
    def hackEnv( self, restore = False ) :
        """
        a trick to reset X509_USER_PROXY when glite is not able to handle
        explicit proxy
        """

        # skip if the env proxy is correct
        if self.cert == '' or self.cert == self.envProxy :
            return

        # apply the X509_USER_PROXY hack
        if restore :
            os.environ["X509_USER_PROXY"] = self.envProxy
        else :
            os.environ["X509_USER_PROXY"] = self.cert

        return

    ##########################################################################
    def mergeJDL( self, jdl, wms='', configfile='' ):
        """
        parse config files, merge jdl and retrieve wms list
        """


        schedClassad = ""
        endpoints = []
        if len( wms ) == 0:
            pass
        elif type( wms ) == str :
            endpoints = [ wms ]
        elif type( wms ) == list :
            endpoints = wms

        if len( endpoints ) == 0 :
            endpoints, schedClassad = self.parseConfig ( configfile )
        else :
            tmp, schedClassad = self.parseConfig ( configfile )

        begin = ''
        jdl.strip()
        if jdl[0] == '[' :
            begin = '[\n'
            jdl = begin + schedClassad + jdl[1:]

        return jdl, endpoints


    ##########################################################################
    def parseConfig ( self, configfile ):
        """
        Utility fuction

        extract entries from glite config files
        """

        cladAddDict = {}
        endpoints = []

        try:
            if ( len(configfile) == 0 ):
                configfile = "%s/etc/%s/glite_wms.conf" \
                             % ( os.environ['GLITE_LOCATION'], self.vo )

            fileh = open( configfile, "r" )
            configClad = fileh.read().strip()
            cladAddDict, endpoints, dummyfile = processClassAd( configClad )
        except  StandardError, err:
            self.warnings.append( "Warning : " + str( err ) )

        if ( len(endpoints) == 0  ) :
            raise SchedulerError( "bad jdl ", "No WMS defined" )

        cladadd = ''
        for k, v in cladAddDict.iteritems():
            cladadd += k + ' = ' + v + ';\n'

        return endpoints, cladadd


    ##########################################################################
    def wmsResolve( self, aliasEndpoints ) :
        """
        resolve Wmproxy from alias
        """

        wmsList = []
        for aliasWms in aliasEndpoints :

            aliasWms = aliasWms.replace("\"", "").strip()
            if  len( aliasWms ) == 0 or aliasWms[0]=='#':
                continue

            # name composition
            st = aliasWms.find( 'https://' )
            if st == 0:
                wmsHostName, post = aliasWms[8:].split(':')
                pre = 'https://'
                post = ':' + post

            else :
                wmsHostName = aliasWms
                pre = 'https://'
                post = ':7443/glite_wms_wmproxy_server'

            try :
                # retrieve all associed ip addresses
                (hostname, aliaslist, ipaddrlist) = \
                           socket.gethostbyname_ex ( wmsHostName )
                for wms in ipaddrlist :                
                    wmsList.append(pre + socket.gethostbyaddr( wms )[0] + post)
            except socket.gaierror, msg:
                self.warnings.append( '%s : %s ' % ( wmsHostName, msg ) )
                self.logging.warning( '%s : %s ' % ( wmsHostName, msg ) )

        return wmsList


    ##########################################################################
    def wmproxyInit( self, wms ) :
        """
        initialize Wmproxy and perform everything needed
        """

        if self.isNewUi :
            # initialize wms connection
            wmpconfig = WMPConfig(wms)

            if self.skipWMSAuth :
                wmpconfig.setAuth(0)
            
            if self.cert != '' :
                wmpconfig.setProxyPath( self.cert )

            wmproxy = Wmproxy(wmpconfig)

        else :
            # initialize wms connection
            wmproxy = Wmproxy(wms, proxy=self.cert)

            if self.skipWMSAuth :
                try :
                    wmproxy.setAuth(0)
                    # UI 3.1: missing method
                except AttributeError:
                    pass

        ### init & return
        wmproxy.soapInit()

        return wmproxy


    ##########################################################################
    def wmproxySubmit( self, jdl, wms, sandboxFileList, workdir ) :
        """
        actual submission function

        provides the interaction with the wmproxy.
        needs some cleaning
        """

        # first check if the sandbox dir can be created
        sandboxDir = os.path.join( workdir, self.SandboxDir )
        localZippedISB = os.path.join( workdir, self.zippedISB )
        if os.path.exists( sandboxDir ) != 0:
            os.system( "rm -rf " + sandboxDir + ' ' + localZippedISB )

        # initialize wmproxy
        self.hackEnv() ### TEMP FIX

        # initialize wms connection
        wmproxy = self.wmproxyInit( wms )
        self.logging.debug( 'DBG for proxy cert=%s X509=%s' % \
               ( self.cert, os.environ.get("X509_USER_PROXY", 'notdefined') ) )

        # register job: time consumng operation
        try:
            task = wmproxy.jobRegister ( jdl, self.delegationId )
        except WMPException, wmpError :
            if wmpError.toString().find('Unable to get delegated Proxy'):
                self.delegateWmsProxy( wmproxy, workdir )
                task = wmproxy.jobRegister ( jdl, self.delegationId )
            else:
                raise
            
        # retrieve parent id
        taskId = str( task.getJobId() )
        if taskId == '':
            raise SchedulerError( "wmproxy.getJobId",
                                  "returned empty collection parent id" )

        # retrieve nodes id
        returnMap = {}
        for job in task.getChildren():
            returnMap[ 
                str( job.getNodeName() ).replace( 'NodeName_', '', 1 ) 
                ] = str( job.getJobId() )

        if returnMap == {} :
            raise SchedulerError( "wmproxy.getChildren",
                                  "returned empty list of node ids" )

        # handle input sandbox :
        if sandboxFileList != '' :

            # get destination
            destURI = wmproxy.getSandboxDestURI(taskId)

            # make directory struct locally
            basedir = os.path.join( self.SandboxDir,
                                    destURI[0].split(self.SandboxDir + '/')[1]
                                    )
            builddir = os.path.join( workdir, basedir )
            os.makedirs( builddir )

            # copy files in the directory
            command = "cp %s %s" % (sandboxFileList, builddir)
            msg, ret = self.ExecuteCommand( command )
            if ret != 0 or msg != '' :
                os.system( "rm -rf " + sandboxDir )
                raise SchedulerError( "cp error", msg, command )

            # zip sandbox + chmod workaround for the wms
            msg, ret = self.ExecuteCommand(
                "chmod 773 " + sandboxDir + "; chmod 773 " + sandboxDir + "/*"
                )
            command = "cd %s; tar pczf %s %s/*; cd - > /dev/null" % \
                      (workdir, self.zippedISB, basedir)
            msg, ret = self.ExecuteCommand( command )
            if ret != 0 or msg != '' :
                os.system( "rm -rf " + sandboxDir + ' ' + localZippedISB )
                raise SchedulerError( "tar error", msg, command )

            try:
                # copy file to the wms (also usable curl)
                #
                # command = "/usr/bin/curl --cert  " + self.cert + \
                #          " --key " + self.cert + \
                #          " --capath /etc/grid-security/certificates " + \
                #          " --upload-file://%s/%s %s/%s "

                command = "globus-url-copy file://%s %s/%s" \
                          % ( localZippedISB, destURI[0], self.zippedISB )
                msg, ret = self.ExecuteCommand(self.proxyString + command)
                if ret != 0 or msg.upper().find("ERROR") >= 0 \
                       or msg.find("wrong format") >= 0 :
                    raise SchedulerError("globus-url-copy error", msg, command)

            except (BaseException, Exception), err:
                os.system( "rm -rf " + sandboxDir + ' ' + localZippedISB )
                raise

        # start job!
        try:
            wmproxy.jobStart(taskId)
        except (BaseException, Exception), err:
            wmproxy.jobPurge(taskId)
            os.system( "rm -rf " + sandboxDir + ' ' + localZippedISB )
            raise
        except :
            os.system( "rm -rf " + sandboxDir + ' ' + localZippedISB )
            raise

        # cleaning up everything: delete temporary files and exit
        self.hackEnv(restore=True) ### TEMP FIX
        if sandboxFileList != '' :
            os.system( "rm -rf " + sandboxDir + ' ' + localZippedISB )

        return taskId, returnMap

    ##########################################################################
    def delegateWmsProxy( self, wmproxy, workdir ):
        """
        delegate proxy to a wms
        """

        # slc5 UI
        if self.isNewUi :
            proxycert = wmproxy.getNewProxyReq(self.delegationId)
            os.environ["PROXY_REQ"] = proxycert
            result = wmproxy.signProxyReqEnv("PROXY_REQ")
            wmproxy.putProxy(self.delegationId, result )
            return

        ### slc4 UI 
        ofile, tmpfile = tempfile.mkstemp(prefix='proxy_del_', dir=workdir)
        os.close( ofile )
        proxycert = wmproxy.getProxyReq(self.delegationId)
        # cmd =  "glite-proxy-cert -o " + tmpfile + " -p '" + proxycert \
        #       + "' "  + self.getProxy()

        os.environ["PROXY_REQUEST"] = proxycert
        cmd =  "glite-proxy-cert -o " + tmpfile + " -e PROXY_REQUEST " #\
              # + self.getUserProxy()
        msg, ret = self.ExecuteCommand( self.proxyString + cmd )

        if ret != 0 or msg.find("Error") >= 0 :
            os.unlink( tmpfile )
            raise SchedulerError("Unable to delegate proxy", msg, cmd)

        proxyres = open( tmpfile )
        wmproxy.putProxy( self.delegationId, ''.join( proxyres.readlines() ) )
        proxyres.close()
        os.unlink( tmpfile )


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

        # decode obj
        jdl, sandboxFileList = self.decode( obj, requirements )

        # return values
        taskId = ''
        returnMap = {}

        # handle wms and prepare jdl
        jdl, endpoints = self.mergeJDL( jdl, service, config )
        endpoints = self.wmsResolve( endpoints )
        if endpoints == [] :
            raise SchedulerError( "failed submission", "empty WMS list" )
 
        # emulate ui round robin
        try :
            import random
            random.shuffle(endpoints)
        except ImportError:
            self.warnings.append(
                "random access to wms not allowed, using sequential access" )
            self.logging.warning(
                "random access to wms not allowed, using sequential access" )

        errors = ''
        success = None
        seen = []

        workdir = tempfile.mkdtemp( prefix = obj['name'], dir = os.getcwd() )

        for wms in endpoints :
            try :
                success = None
                wms = wms.replace("\"", "").strip()
                if  len( wms ) == 0 or wms[0]=='#' or wms in seen:
                    continue
                else :
                    seen.append( wms)
                self.logging.debug( "Submitting to : %s" % wms )
                taskId, returnMap = \
                        self.wmproxySubmit( jdl, wms, sandboxFileList, workdir)
                success = wms
                self.logging.debug( "Submitted successfully to : %s" % wms )
                break
            
            # typical exception
            except BaseException, err:
                wmserr = 'failed to submit to %s : %s ;\n' % \
                         ( wms,  formatWmpError( err, self.isNewUi ) )
                errors += wmserr
                self.logging.debug( wmserr )
                continue
            
            ### very very bad! wms exceptions...
            except Exception, err:
                wmserr = 'failed to submit to %s : %s ;\n' % ( wms, err )
                errors += wmserr
                self.logging.debug( wmserr )
                continue
            
            except :
                import sys
                if isinstance( sys.exc_type, str):
                    wmserr = 'failed SSL auth to wms ' + wms + \
                             ' : ' +  str(sys.exc_info()[0]) + ';\n'
                    errors += wmserr
                    self.logging.debug( wmserr )
                    continue
                raise

        # clean files
        os.system("rm -rf " + workdir)

        # log warnings
        obj.warnings.extend( self.warnings )

        # check success consistency
        if success is not None :
                
            # further check on submission consistency: valid grid ids
            if returnMap == {} or taskId == '':
                errors += "UNKNOWN ERROR: empty ids! WMS reported as success: "
                errors += " {%s}; List of WMS failed {%s}" \
                          % ( success, seen )
                success = None

            # if ok, log eventual failed submissions
            elif errors != '' :
                obj.warnings.append( errors )


        # if submission failed, raise error
        if success is None :
            raise SchedulerError( "failed submission", errors )

        # handle jobs
        for job in obj.jobs :

            # wmproxy converts . to _ in jobIds - convert back
            if job['name'].count('.'):
                wmproxyName = job['name'].replace('.', '_')
                returnMap[job['name']] = returnMap.pop(wmproxyName)

        return returnMap, taskId, success


    ##########################################################################
    def getOutput( self, obj, outdir='' ):
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

            if outdir == '' and obj['outputDirectory'] is not None:
                outdir = obj['outputDirectory']

            if outdir != '' and not os.path.exists( outdir ) :
                raise SchedulerError( 'Permission denied', \
                                      'Unable to write files in ' + outdir )

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
        self.hackEnv() ### TEMP FIX

        # initialize wms connection
        wmproxy = self.wmproxyInit( wms )
        self.logging.debug( 'DBG for proxy cert=%s X509=%s' % \
               ( self.cert, os.environ.get("X509_USER_PROXY", 'notdefined') ) )

        # loop over jobs
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

            # typical exception
            except BaseException, err:
                output = formatWmpError( err, self.isNewUi )

                # proxy expired: skip!
                if output.find( 'Error with credential' ) != -1 :
                    job.runningJob.errors.append( output )
                    # job.runningJob['statusHistory'].append(
                    #     'Error with credential' )
                    continue

                # purged: probably already retrieved. Archive
                elif output.find( "has been purged" ) != -1 :
                    job.runningJob.warnings.append( 
                        'Job has been purged, recovering status' )
                    self.logging.warning(
                        'Job has been purged, recovering status' )
                    continue

                # not ready for GO: waiting for next round
                elif output.find( "Job current status doesn" ) != -1 :
                    self.logging.warning( output )
                    job.runningJob.errors.append( output )
                    continue

                # not ready for GO: waiting for next round
                else :
                    job.runningJob.errors.append( output )
                    self.logging.error( output )
                    continue

            ### very very bad! wms exceptions...
            except Exception, err:
                raise SchedulerError('Failed SSL auth to wms', err)
            except :
                import sys
                if isinstance( sys.exc_type, str):
                    raise SchedulerError('Failed SSL auth to wms', \
                                         str(sys.exc_info()[0]) )
                raise


            # retrieve files
            retrieved = 0
            for m in filelist:

                # ugly trick for empty fields...
                if m['name'].strip() == '' :
                    joberr +=  'empty filename; '
                    continue

                # ugly error: nothing there!
                try:
                    size = int( m['size'] )
                except ValueError :
                    size = 0

                # avoid globus-url-copy for empty files
                if size == 0 :
                    joberr +=  'file ' + os.path.basename( m['name'] ) \
                             + ' reported has zero size;'
                    checkSize = False
                else:
                    checkSize = True

                # retrieve file
                dest = os.path.join( outdir + '/' + \
                                     os.path.basename(m['name']) )
                command = "globus-url-copy -verbose " + m['name'] \
                          + " file://" + dest
                msg, ret = self.ExecuteCommand(self.proxyString + command)

                if checkSize :
                    if ret != 0 or msg.upper().find("ERROR") >= 0 \
                           or msg.find("wrong format") >= 0 :
                        joberr = '[ ' + command + ' ] : ' + msg + '; '
                        continue

                    # check file size
                    if os.path.getsize(dest) !=  size  :
                        joberr =  'size mismatch : expected ' \
                                 + str( os.path.getsize(dest) ) \
                                 + ' got ' + m['size'] + '; '

                # update files counter
                retrieved += 1

            # no files?
            if retrieved == 0:
                self.logging.warning( 'Warning: non files to be retrieved' )
                job.runningJob.errors.append(
                    'Warning: no files to be retrieved' )

            # got errors?
            if joberr != '' :
                self.logging.warning( 'problems with output retrieval' )
                job.runningJob.errors.append( joberr )

                
            if job.runningJob.errors is None or job.runningJob.errors == [] :
                self.logging.debug( 'Output successfully retrieved' )
                # try to purge files
                try :
                    wmproxy.jobPurge( jobId )
                except BaseException, err:
                    job.runningJob.warnings.append("unable to purge WMS")
                    self.logging.warning( "unable to purge WMS" )
                except :
                    job.runningJob.warnings.append("unable to purge WMS")
                    self.logging.warning( "unable to purge WMS" )

        self.hackEnv(restore=True) ### TEMP FIX


    ##########################################################################
    def kill( self, obj ):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        # the object passed is a job
        if type(obj) == Job :

            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # kill the job
            self.doWMScancel( [ obj ], obj.runningJob['service']  )

            # errors?
            if obj.runningJob.isError() :
                raise SchedulerError( str( obj.runningJob.errors[0][0], \
                                           obj.runningJob.errors[0][1] ))

        # the object passed is a Task
        elif type(obj) == Task :

            # retrieve scheduler id list
            schedIdList = {}
            for job in obj.jobs:
                if self.valid( job.runningJob ):
                    if not schedIdList.has_key( job.runningJob['service'] ) :
                        schedIdList[job.runningJob['service']] = []
                    schedIdList[job.runningJob['service']].append( job )

            # retrieve output for all jobs
            for service, idList in schedIdList.iteritems() :
                if service is not None:
                    self.doWMScancel( idList, service )

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    ##########################################################################
    def doWMScancel( self, jobList, service):
        """
        Kill jobs submitted to a given WMS. Does not perform status check
        """

        # skip empty endpoint
        wms = service.strip()
        if len(wms) == 0 :
            return

        # look for a well formed name
        if wms.find( 'https') < 0 :
            wms = 'https://' + wms + ':7443/glite_wms_wmproxy_server'

        # initialize wmproxy
        self.hackEnv() ### TEMP FIX

        # initialize wms connection
        self.logging.debug( 'DBG for proxy cert=%s X509=%s' % \
               ( self.cert, os.environ.get("X509_USER_PROXY", 'notdefined') ) )
        wmproxy = self.wmproxyInit( wms )

        # loop over jobs
        for job in jobList:

            # skip malformed id
            jobId = str( job.runningJob['schedulerId'] ).strip()
            if jobId is None or len(jobId) == 0 :
                continue

            # eventual error container
            joberr = ''

            # get file list
            try :
                wmproxy.jobCancel( jobId )

            # typical exception
            except BaseException, err:
                output = formatWmpError( err, self.isNewUi )

                # purged: probably already retrieved. Archive
                if output.find( "Cancel has already been requested" ) != -1 :
                    job.runningJob.warnings.append( 
                        'Cancel has already been requested, recovering status')
                    self.logging.warning( 
                        'Cancel has already been requested, recovering status')
                    continue
                else :
                    job.runningJob.errors.append( output )

            ### very very bad! wms exceptions...
            except Exception, err:
                raise SchedulerError('Failed SSL auth to wms', err)
            except :
                import sys
                if isinstance( sys.exc_type, str):
                    raise SchedulerError('Failed SSL auth to wms', \
                                         str(sys.exc_info()[0]) )
                raise

            try :
                wmproxy.jobPurge( jobId )
            except BaseException, err:
                # job.runningJob.warnings.append("unable to purge WMS")
                # job.runningJob['statusHistory'].append("unable to purge WMS")
                continue
            except :
                continue

        self.hackEnv(restore=True) ### TEMP FIX

    ##########################################################################
    ###
    ###     def killCheck( self, schedIdList, idType = 'node' ):
    ###         """
    ###         Kill jobs querying WMS to LB, if the job status allows
    ###         If a list of parent id is used, must be idType='parent'
    ###         """
    ###
    ###         jobs = []
    ###         if len( schedIdList ) == 0:
    ###             return
    ###         elif type( schedIdList ) == str :
    ###             jobs = [ schedIdList ]
    ###         elif type( schedIdList ) == list :
    ###             jobs = schedIdList
    ###
    ###         # retrieve wms
    ###         from ProdCommon.BossLite.Scheduler.GLiteLBQuery import groupByWMS
    ###         endpoints = groupByWMS(
    ###             jobs, self.cert, idType, \
    ###             status_list = ['Done', 'Aborted','Cancelled'],\
    ###             allow = False
    ###             )
    ###
    ###         # actual kill
    ###         for wms, schedIdList in endpoints.iteritems():
    ###             try :
    ###                 self.kill( wms, schedIdList )
    ###             except StandardError:
    ###                 pass
    ###                 # for job in schedIdList:
    ###                 #     job.runningJob.errors.append( "failed" )
    ###
    ###
    ##########################################################################
    def purgeService( self, obj ):
        """
        Purge job (even bulk) from wms
        """

        # the object passed is a job
        if type(obj) == Job :

            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # kill the job
            self.purgeWMS( [ obj ], obj.runningJob['service']  )

            # errors?
            if obj.runningJob.isError() :
                raise SchedulerError( str( obj.runningJob.errors[0][0], \
                                           obj.runningJob.errors[0][1] ))

        # the object passed is a Task
        elif type(obj) == Task :

            # retrieve scheduler id list
            schedIdList = {}
            for job in obj.jobs:
                if self.valid( job.runningJob ):
                    if not schedIdList.has_key( job.runningJob['service'] ) :
                        schedIdList[job.runningJob['service']] = []
                    schedIdList[job.runningJob['service']].append( job )

            # retrieve output for all jobs
            for service, idList in schedIdList.iteritems() :
                self.purgeWMS( idList, service )

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    ##########################################################################
    def purgeWMS( self, jobList, service ):
        """
        Purge jobs submitted to a given WMS. Does not perform status check
        """

        # skip empty endpoint
        wms = service.strip()
        if len(wms) == 0 :
            return

        # look for a well formed name
        if wms.find( 'https') < 0 :
            wms = 'https://' + wms + ':7443/glite_wms_wmproxy_server'

        # initialize wmproxy
        self.hackEnv() ### TEMP FIX

        # initialize wms connection
        wmproxy = self.wmproxyInit( wms )
        self.logging.debug( 'DBG for proxy cert=%s X509=%s' % \
               ( self.cert, os.environ.get("X509_USER_PROXY", 'notdefined') ) )

        # loop over jobs
        for job in jobList:

            # skip malformed id
            jobId = str( job.runningJob['schedulerId'] ).strip()
            if jobId is None or len(jobId) == 0 :
                continue

            # purge
            try :
                wmproxy.jobPurge( jobId )
            except BaseException, err:
                # job.runningJob.warnings.append("unable to purge WMS")
                # job.runningJob['statusHistory'].append("unable to purge WMS")
                continue
            except :
                continue

        self.hackEnv(restore=True) ### TEMP FIX


    ##########################################################################
    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        resources list match
        """
        jdl = self.decode( obj, requirements)[0]
        matchingCEs = []
        errorList = []

        jdl, endpoints = self.mergeJDL( jdl, service, config)

        # handle wms
        if endpoints == [] :
            raise SchedulerError( "failed submission", "empty WMS list" )

        # emulate ui round robin
        try :
            import random
            random.shuffle(endpoints)
        except ImportError:
            errorList.append(
                "random access to wms not allowed, using sequential access" )

        # jdl ready!
        seen = []

        workdir = tempfile.mkdtemp( prefix = obj['name'], dir = os.getcwd() )

        for wms in self.wmsResolve( endpoints ) :
            try :
                wms = wms.replace("\"", "").strip()
                if  len( wms ) == 0 or wms[0]=='#' or wms in seen:
                    continue
                else :
                    seen.append( wms)
                errorList.append( "ListMatch to : " + wms )

                # delegate proxy
                self.delegateWmsProxy( wms )

                # initialize wms connection
                wmproxy = self.wmproxyInit( wms, workdir )

                # list match
                matchingCEs = wmproxy.jobListMatch(jdl, "bossproxy")
                if matchingCEs != None and len ( matchingCEs ) != 0 :
                    break
                else :
                    errorList.append( "No results for listMatch" )
            except BaseException, err:
                continue
            except SchedulerError, err:
                continue

        os.system("rm -rf " + workdir)

        # log warnings
        for job in obj.jobs :
            job.runningJob.errors.extend( errorList )

        return matchingCEs


    ##########################################################################
    def postMortem( self, schedulerId, outfile, service):
        """
        perform scheduler self.logging-info

        """
        command = "glite-wms-job-logging-info -v 3 " + schedulerId + \
                  " > " + outfile

        return self.ExecuteCommand( self.proxyString + command )[0]


    ##########################################################################
    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        """

        # the object passed is a Task:
        #   check whether parent id are provided, make a list of ids
        #     and check the status
        if type(obj) == Task :

            from ProdCommon.BossLite.Scheduler.GLiteLBQuery import GLiteLBQuery
            lbInstance = GLiteLBQuery()

            # query performed through single job ids
            if objType == 'node' :

                self.hackEnv() ### TEMP FIX
                lbInstance.checkJobs( obj, self.invalidList )
                self.hackEnv( restore = True ) ### TEMP FIX

            # query performed through a bulk id
            elif objType == 'parent' :

                # jobId for remapping
                jobIds = {}

                # parent Ids for status query
                parentIds = []

                # counter for job position in list
                count = 0

                # loop!
                for job in obj.jobs :

                    # consider just valid jobs
                    if self.valid( job.runningJob ) :

                        # append in joblist
                        jobIds[ str(job.runningJob['schedulerId']) ] = count

                        # update unique parent ids list
                        if job.runningJob['schedulerParentId'] \
                               not in parentIds:

                            parentIds.append(
                                str(job.runningJob['schedulerParentId'] )
                                )

                    count += 1

                self.hackEnv() ### TEMP FIX
                lbInstance.checkJobsBulk( obj, jobIds, parentIds )
                self.hackEnv( restore = True ) ### TEMP FIX


        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


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
        jdl = self.decode( obj, requirements )[0]

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

        # output bypass WMS?
        #if task['outputDirectory'] is not None and \
        #       task['outputDirectory'].find('gsiftp://') >= 0 :
        #    jdl += 'OutputSandboxBaseDestURI = "%s";\n' % \
        #           task['outputDirectory']

        # output files handling
        outfiles = ''
        for filePath in job['outputFiles'] :
            if filePath == '' :
                continue
            if self.renameOutputFiles :
                outfiles += '"' + filePath + '_' + \
                            str(job.runningJob[ 'submission' ]) + '",'
            else :
                outfiles += '"' + filePath + '",'

        if len( outfiles ) != 0 :
            jdl += 'OutputSandbox = {%s};\n' % outfiles[:-1]

        # extra job attributes
        if job.runningJob is not None \
               and job.runningJob[ 'schedulerAttributes' ] is not None :
            jdl += job.runningJob[ 'schedulerAttributes' ]

        # blindly append user requirements
        jdl += requirements + '\n]\n'

        # return values
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
        globalSandbox = ''
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
                    if ifile.strip() == '' :
                        continue
                    filename = os.path.abspath( ifile )
                    globalSandbox += '"file://' + filename + '",'
                    filelist += filename + ' '
                    commonFiles += "root.inputsandbox[%d]," % ISBindex
                    ISBindex += 1
        else :
            # files are elsewhere, just add their composed path
            if task['globalSandbox'] is not None :
                jdl += 'InputSandboxBaseURI = "%s";\n' % task['startDirectory']
                for ifile in task['globalSandbox'].split(','):
                    if ifile.strip() == '' :
                        continue
                    if ifile.find( 'file:/' ) == 0:
                        globalSandbox += '"' + ifile + '",'
                        
                        filelist += ifile[5:] + ' '
                        commonFiles += "root.inputsandbox[%d]," % ISBindex
                        ISBindex += 1
                        continue
                    if ifile[0] == '/':
                        ifile = ifile[1:]
                    commonFiles += '"' + ifile + '",'

        # output bypass WMS?
        if task['outputDirectory'] is not None and \
               task['outputDirectory'].find('gsiftp://') >= 0 :
            jdl += 'OutputSandboxBaseDestURI = "%s";\n' % \
                   task['outputDirectory']

        # single job definition
        jdl += "Nodes = {\n"
        for job in task.jobs :
            jdl += '[\n'
            jdl += 'NodeName   = "NodeName_%s";\n' % job[ 'name' ]
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
            for filePath in job['outputFiles'] :
                if filePath == '' :
                    continue
                if self.renameOutputFiles :
                    outfiles += '"' + filePath + '_' + \
                                str(job.runningJob[ 'submission' ]) + '",'
                else :
                    outfiles += '"' + filePath + '",'

            if len( outfiles ) != 0 :
                jdl += 'OutputSandbox = {%s};\n' % outfiles[:-1]

            # job input files handling:
            # add their name in the global sanbox and put a reference
            localfiles = commonFiles
            if task['startDirectory'] is None \
                   or task['startDirectory'][0] == '/':
                # files are stored locally, compose with 'file://'
                for filePath in job['fullPathInputFiles']:
                    if filePath != '' :
                        localfiles += "root.inputsandbox[%d]," % ISBindex
                    globalSandbox += '"file://' + filePath + '",'
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
        if globalSandbox != '' :
            jdl += "InputSandbox = {%s};\n"% (globalSandbox[:-1])

        # blindly append user requirements
        try :
            requirements = requirements.strip()
            while requirements[0] == '[':
                requirements = requirements[1:-1].strip()
            jdl += '\n' + requirements + '\n'
        except Exception:
            pass

        # useful attributes if the transfer is not direct to the WN
        if filelist != '':
            jdl += 'AllowZippedISB = true;\n'
            jdl += 'ZippedISB = "%s";\n' % self.zippedISB

        # close jdl
        jdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        jdl += "\n]\n"

        # return values
        return jdl, filelist


    ##########################################################################
    def lcgInfo(self, tags, vos, seList=None, blacklist=None, whitelist=None, full=False):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """

        result = []
        for fqan in vos :
            res = self.lcgInfoVo( tags, fqan, seList,
                                  blacklist, whitelist, full)
            if not full and res != [] :
                return res
            else :
                for value in res :
                    if value in result :
                        continue
                    result.append( value )

        return result

    ##########################################################################
    def lcgInfoVo(self, tags, fqan, seList=None, blacklist=None, whitelist=None, full=False):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """

        celist = []

        # set to None invalid entries
        if seList == [''] or seList == []:
            seList = None
        # set to None invalid entries
        if whitelist == [''] or whitelist == []:
            whitelist = None
        # set to [] invalid entries so that the lopp does't need checks
        if blacklist == [''] or blacklist == None:
            blacklist = []

        if len( tags ) != 0 :
            query =  ','.join( ["Tag=%s" % tag for tag in tags ] ) + \
                    ',CEStatus=Production'
        else :
            query = 'CEStatus=Production'

        if seList == None :
            command = "lcg-info --vo " + fqan + " --list-ce --query " + \
                       "\'" + query + "\' --sed"
            self.logging.debug('issuing : %s' % command)

            out, ret = self.ExecuteCommand( self.proxyString + command )
            for ce in out.split() :
                # blacklist
                passblack = 1
                if ce.find( "blah" ) == -1:
                    for ceb in blacklist :
                        if ce.find(ceb) >= 0:
                            passblack = 0
                # whitelist if surviving the blacklist selection
                if passblack:
                    if whitelist is None:
                        celist.append( ce )
                    elif len(whitelist) == 0:
                        celist.append( ce )
                    else:
                        for cew in whitelist:
                            if ce.find(cew) != -1:
                                celist.append( ce )
            return celist

        for se in seList :
            singleComm = "lcg-info --vo " + fqan + \
                         " --list-ce --query " + \
                         "\'" + query + ",CloseSE="+ se + "\' --sed"
            self.logging.debug('issuing : %s' % singleComm)

            out, ret = self.ExecuteCommand( self.proxyString + singleComm )
            for ce in out.split() :
                # blacklist
                passblack = 1
                if ce.find( "blah" ) == -1:
                    for ceb in blacklist :
                        if ce.find(ceb) != -1:
                            passblack = 0
                # whitelist if surviving the blacklist selection
                if passblack:
                    if whitelist is None:
                        celist.append( ce )
                    elif len(whitelist) == 0:
                        celist.append( ce )
                    else:
                        for cew in whitelist:
                            if ce.find(cew) >= 0:
                                celist.append( ce )

            # a site matching is enough
            if not full and celist != []:
                break
        return celist


    def delegateProxy( self ) :
        """
        _delegateProxy_
        """

        workdir = tempfile.mkdtemp( prefix = 'delegation', dir = os.getcwd() )
        config, endpoints = self.mergeJDL('[]', self.service, self.config)

        seen = []
        for wms in self.wmsResolve( endpoints ) :
            try :
                if  len( wms ) == 0 or wms[0] == '#' or wms in seen:
                    continue
                else :
                    seen.append( wms)
                wmproxy = self.wmproxyInit( wms )
                self.delegateWmsProxy( wmproxy, workdir )
                self.logging.debug('Delegated proxy to %s' % wms)
            except BaseException, err:
                self.logging.error( 'BossLite BaseException: failed to delegate proxy to ' + wms + \
                                    ' : ' + formatWmpError( err, self.isNewUi ) )
                continue

            except Exception, err:
                self.logging.error( 'BossLite Exception: failed to delegate proxy to ' + wms + \
                               ' : ' + str( err ) )
                continue

            except :
                self.logging.error( 'BossLite generic exception: failed to delegate proxy to ' + wms )
                continue

        os.system("rm -rf " + workdir)


