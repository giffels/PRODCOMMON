#!/usr/bin/env python
"""
basic glite CLI interaction class
"""


__revision__ = "$Id: SchedulerGLite.py,v 1.7 2009/11/19 15:27:19 gcodispo Exp $"
__version__ = "$Revision: 1.7 $"

import os
import tempfile
from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob


##########################################################################
class SchedulerGLite (SchedulerInterface) :
    """
    basic class to handle glite jobs
    """
    def __init__( self, **args):

        # call super class init method
        super(SchedulerGLite, self).__init__(**args)

        # some initializations
        self.proxyString = ''
        self.envProxy = os.environ.get("X509_USER_PROXY",'')
        self.warnings = []

        # # skipWMSAuth
        # self.skipWMSAuth = args.get("skipWMSAuth", 0)
        # self.skipWMSAuth = int( self.skipWMSAuth )

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

    def delegateWMSProxy( self, wms ):
        """
        delegate proxy to a wms
        """

        command = ";glite-wms-job-delegate-proxy -d " + self.delegationId \
                  + " --endpoint " + wms
        msg, ret = self.ExecuteCommand( self.proxyString + command )

        if ret != 0 or msg.find("Error -") >= 0 :
            self.logging.warning( "Warning : %s" % msg )

    ##########################################################################

    def delegateProxy( self ):
        """
        delegate proxy to _all_ wms
        """

        command = ";glite-wms-job-delegate-proxy -d " + self.delegationId

        msg, ret = self.ExecuteCommand( self.proxyString + command )

        if ret != 0 or msg.find("Error -") >= 0 :
            self.logging.warning( "Warning : %s" % msg )


    ##########################################################################
    def submit( self, obj, requirements='', config ='', service='' ):
        """
        submit a jdl to glite
        ends with a call to retrieve wms and job,gridid asssociation
        """

        # decode object
        jdl = self.decode( obj, requirements )
        
        # write a jdl tmpFile
        workdir = tempfile.mkdtemp( prefix = obj['name'], dir = os.getcwd() )
        tmp, fname = tempfile.mkstemp( workdir, "glite_bulk_", os.getcwd() )
        tmpFile = open( fname, 'w')
        tmpFile.write( jdl )
        tmpFile.close()
        command = ''
        
        # delegate proxy
        if self.delegationId != "" :
            command += "glite-wms-job-submit -d " + self.delegationId
            self.delegateProxy( service )
        else :
            command += "glite-wms-job-submit -a "
        
        if len(config) != 0 :
            command += " -c " + config

        ###
        if service != '' :
            command += ' -e ' + service

        command += ' ' + fname
        out, ret = self.ExecuteCommand( self.proxyString + command )
        
        try:
            c = out.split("Your job identifier is:")[1].strip()
            taskId = c.split("=")[0].strip()
        except IndexError:
            raise SchedulerError( 'wrong parent id',  out )
        
        self.logging.info( "Your job identifier is: %s" % taskId )
        os.unlink( fname )


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
            command = "glite-wms-job-output --noint --dir " \
                      + outdir + " " + jobId
            out, ret = self.ExecuteCommand( self.proxyString + command )
            if out.find("have been successfully retrieved") == -1 :
                raise SchedulerError( 'retrieved', out )

    ##########################################################################
    def purgeService( self, obj ):
        """
        Purge job (even bulk) from wms
        """

        ### TO DO
        ## Implement as getoutput where the "No output files ..."
        # is not an error condition but the expected status

        raise NotImplementedError
        
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

        command = "glite-wms-job-cancel --noint " + schedIdList
        out, ret = self.ExecuteCommand( self.proxyString + command )
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


        out, ret = self.ExecuteCommand( self.proxyString + command )

        try:
            out = out.split("CEId")[1].strip()
        except IndexError:
            raise SchedulerError( 'IndexError', out )

        return out.split()
        

    ##########################################################################

    def postMortem( self, schedulerId, outfile, service):
        """
        perform scheduler logging-info
        
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
        return jdl

    ##########################################################################
    def collectionJdlFile ( self, task, requirements='' ):
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

        # close jdl
        jdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        jdl += "\n]\n"

        # return values
        return jdl

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

