#!/usr/bin/env python
"""
gLite CLI interaction class through JSON formatted output
"""

__revision__ = "$Id: SchedulerGLite.py,v 2.8 2009/12/17 23:23:17 spigafi Exp $"
__version__ = "$Revision: 2.8 $"
__author__ = "filippo.spiga@cern.ch"

import os
import tempfile
import re
import simplejson as json

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

from json.decoder import JSONDecoder

##########################################################################

class BossliteJsonDecoder(JSONDecoder):
    """
    Override JSON decode
    """

    def __init__(self):
        
        # call super
        super(BossliteJsonDecoder, self).__init__()
        
        # cache pattern to optimize reg-exp substitution
        self.pattern1 = re.compile('\{,[\s]*([a-zA-Z0-9_\-])')
        self.pattern2 = re.compile(':[\s]([a-zA-Z_\-])')
        self.pattern3 = re.compile(
                    '[\s]*([a-zA-Z0-9_\-]*),[\s]*([a-zA-Z0-9_\-]*)"')
        self.pattern4 = re.compile(
                    '[\s]*([a-zA-Z0-9_\-]*),[\s]*([a-zA-Z0-9_\-]*):')
        self.pattern5 = re.compile(',[\s]*}(?!"[\s]*[a-zA-Z0-9_\-]*)')
        self.pattern6 = re.compile('([a-zA-Z0-9_\-])}')

    
    def decodeSubmit(self, jsonString):
        """
        specialized method to decode JSON output of glite-wms-job-submit
        """
        
        # pre-processing the string before decoding        
        toParse = jsonString.replace( '\n' , ',' )
        toParse = self.pattern1.sub(r'{ "\1', toParse[:-1] )
        toParse = self.pattern2.sub(r'":"\1', toParse )
        toParse = self.pattern3.sub(r'\1","\2"', toParse )
        toParse = self.pattern4.sub(r'\1","\2":', toParse )
        toParse = self.pattern5.sub(r'}', toParse)
        toParse = self.pattern6.sub(r'\1"}', toParse)
        
        parsedJson = self.decode(toParse)
        
        return parsedJson  

    
##########################################################################

class SchedulerGLite(SchedulerInterface) :
    """
    basic class to handle gLite jobs using CLI + JSON 
    formatted output to interact with the WMS
    """
    
    def __init__( self, **args):

        # call super class init method
        super(SchedulerGLite, self).__init__(**args)

        # some initializations
        self.warnings = []

        # typical options
        self.vo = args.get( "vo", "cms" )
        self.service = args.get( "service", "" )
        self.config = args.get( "config", "" )
        self.delegationId = args.get( "proxyname", "bossproxy" )

        # rename output files with submission number
        self.renameOutputFiles = args.get( "renameOutputFiles", 0 )
        self.renameOutputFiles = int( self.renameOutputFiles )
        
        # x509 string for CLI commands
        if self.cert != '':
            self.proxyString = "env X509_USER_PROXY=" + self.cert + ' '
        else :
            self.proxyString = ''
            
        # this section requires an improvement....    
        if os.environ.get('CRABDIR') :
            self.commandQueryPath = os.environ.get('CRABDIR') + \
                                    '/external/ProdCommon/BossLite/Scheduler/'
        elif os.environ.get('PRODCOMMON_ROOT') :
            self.commandQueryPath = os.environ.get('PRODCOMMON_ROOT') + \
                                        '/lib/ProdCommon/BossLite/Scheduler/'
        else :
            # Impossible to locate GLiteQueryStatus.py ...
            raise SchedulerError('Impossible to locate GLiteQueryStatus.py ')
        
        gliteLocation = os.environ.get('GLITE_LOCATION').split('glite')[0]
        gliteUi = '%s/etc/profile.d/grid-env.sh ' % gliteLocation
        self.prefixCommandQuery = 'unset LD_LIBRARY_PATH;' + \
            'export PATH=/usr/bin:/bin; source /etc/profile;' \
            'source %s' % gliteUi + \
            ';export PYTHONPATH=${PYTHONPATH}:${GLITE_LOCATION}/lib64; '

        # cache pattern to optimize reg-exp substitution
        self.pathPattern = re.compile('location:([\S]*)$', re.M)
        
        # init BossliteJsonDecoder specialized class
        self.myJSONDecoder = BossliteJsonDecoder()

        # Raise an error if UI is old than 3.2 ...
        version, ret = self.ExecuteCommand( 'glite-version' )
        version = version.strip()
        if version.find( '3.2' ) != 0 :
            raise SchedulerError( 'SchedulerGLite is allowed on UI >3.2' )
        
    ##########################################################################

    def delegateProxy( self, wms = '' ):
        """
        delegate proxy to _all_ wms or to specific one (if explicitly passed)
        """

        command = "glite-wms-job-delegate-proxy -d " + self.delegationId

        # inherited from delegateWMSProxy() method ...
        if wms :
            command += " --endpoint " + wms
        
        
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
        tmp, fname = tempfile.mkstemp( suffix = '.jdl', prefix = obj['name'],
                                       dir = os.getcwd() )
        tmpFile = open( fname, 'w')
        tmpFile.write( jdl )
        tmpFile.close()
        
        # delegate proxy?
        if self.delegationId != "" :
            command = "glite-wms-job-submit --json -d " \
                                                + self.delegationId
            self.delegateProxy()
        else :
            command = "glite-wms-job-submit --json -a "
        
        if len(config) != 0 :
            command += " -c " + config

        if service != '' :
            command += ' -e ' + service

        command += ' ' + fname
        out, ret = self.ExecuteCommand( self.proxyString + command )
        
        if ret != 0 :
            raise SchedulerError('error executing glite-wms-job-submit', out)
        
        try:
            
            jOut = self.myJSONDecoder.decodeSubmit(out)
            
        except ValueError:
            raise SchedulerError('error parsing JSON output',  out)
             
        os.unlink( fname )

        returnMap = {}
        if type(obj) == Task:
            self.logging.debug("Your job identifier is: %s" % jOut['parent'])
            
            for child in jOut['children'].keys() :
                returnMap[str(child.replace('NodeName_', '', 1))] = \
                                                str(jOut['children'][child])
            
            return returnMap, str(jOut['parent']), str(jOut['endpoint']) 
        elif type(obj) == Job:
            # usually we submit collections.....
            self.logging.debug("Your job identifier is: %s" % jOut['jobid'])
            
            returnMap[str(child.replace('NodeName_', '', 1))] = \
                                                str(jOut['children'][child])
            
            return returnMap, str(jOut['parent']), str(jOut['endpoint'])
        else : 
            raise SchedulerError( 'unexpected error',  type(obj) )
        
    ##########################################################################

    def getOutput( self, obj, outdir='' ):
        """
        retrieve job output
        """

        if type(obj) == Job :
            
            # check for the RunningJob integrity
            if not self.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))
            
            # the object passed is a valid Job, let's go on ...
                
            command = "glite-wms-job-output --json --noint " \
                                + obj.runningJob['schedulerId']
            
            out, ret = self.ExecuteCommand( self.proxyString + command ) 
                
            if ret == 1 :
                if out.find("Proxy File Not Found") != -1 :
                    # Proxy missing
                    # # adapting the error string for JobOutput requirements
                    obj.runningJob.errors.append("Proxy Missing")
                elif out.find("Output files already retrieved") != -1 : 
                    # Output files already retrieved --> Archive!
                    self.logging.warning( obj.runningJob['schedulerId'] + \
                      ' output already retrieved.' )
                    obj.runningJob.warnings.append("Job has been purged, " + \
                                                        "recovering status")
                elif out.find("Output not yet Ready") != -1 :
                    # Output not yet ready
                    self.logging.warning( obj.runningJob['schedulerId'] + \
                      ' output not yet ready' )
                    # adapting the error string with JobOutput requirements
                    obj.runningJob.errors.append("Job current status doesn")
                else : 
                    self.logging.error( out )
                    obj.runningJob.errors.append( out )
                                           
            elif ret == 0 and out.find("result: success") == -1 :
                # Excluding all the previous cases however something went wrong
                self.logging.error( obj.runningJob['schedulerId'] + \
                      ' problems during getOutput operation.' )
                obj.runningJob.errors.append(out)     
            
            else :
                # Output successfully retrieved without problems
                
                # -> workaround for gLite UI 3.2 
                #    glite-wms-job-output CLI behaviour
                tmp = re.search(self.pathPattern, out)
                uniqueString = str(os.path.basename(tmp.group(1)))
                
                command = "cp -R /tmp/" + uniqueString + "/* " + outdir + "/"
                os.system( command )
                
                command = "rm -rf /tmp/" + uniqueString
                os.system( command )
                # 
                
                self.logging.debug("Output of %s successfully retrieved" 
                                        % str(obj.runningJob['schedulerId'])) 
            
            if obj.runningJob.isError() :
                raise SchedulerError( obj.runningJob.errors[0][0], \
                                           obj.runningJob.errors[0][1] )
                    
        elif type(obj) == Task :
            
            # the object passed is a Task
            
            for selJob in obj.jobs:
                
                if not self.valid( selJob.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint " + \
                          selJob.runningJob['schedulerId']
                
                out, ret = self.ExecuteCommand( self.proxyString + command )

                if ret == 1 :
                    if out.find("Proxy File Not Found") != -1 :
                        # Proxy missing
                        # adapting the error string for JobOutput requirements
                        selJob.runningJob.errors.append("Proxy Missing")
                    elif out.find("Output files already retrieved") != -1 : 
                        # Output files already retrieved --> Archive!
                        self.logging.warning( 
                                    selJob.runningJob['schedulerId'] + \
                                                ' output already retrieved.' )
                        selJob.runningJob.warnings.append(
                                    "Job has been purged, recovering status")
                    elif out.find("Output not yet Ready") != -1 :
                        # Output not yet ready
                        self.logging.warning( 
                                    selJob.runningJob['schedulerId'] + \
                                                    ' output not yet ready' )
                        # adapting the error string with JobOutput requirements
                        selJob.runningJob.errors.append(
                                            "Job current status doesn")
                    else : 
                        self.logging.error( out )
                        selJob.runningJob.errors.append( out )
                                               
                elif ret == 0 and out.find("result: success") == -1 :
                    # Excluding all previous cases however something went wrong
                    self.logging.error( selJob.runningJob['schedulerId'] + \
                          ' problems during getOutput operation.' )
                    selJob.runningJob.errors.append(out)   
                
                else :
                    # Output successfully retrieved without problems
                    
                    # -> workaround for gLite UI 3.2 
                    #    glite-wms-job-output CLI behaviour
                    tmp = re.search(self.pathPattern, out)
                    uniqueString = str(os.path.basename(tmp.group(1)))
                    
                    command = "cp -R /tmp/" + uniqueString + \
                                                "/* " + outdir + "/"
                    os.system( command )
                    
                    command = "rm -rf /tmp/" + uniqueString
                    os.system( command )
                    # 
                    
                    self.logging.debug("Output of %s successfully retrieved" 
                                % str(selJob.runningJob['schedulerId']))
        
        else:
             # unknown object type
            raise SchedulerError('wrong argument type', str( type(obj) ))      

    ##########################################################################
    
    def purgeService( self, obj ):
        """
        Purge job (even bulk) from wms
        """

        # Implement as getOutput where the "No output files ..."
        # is not an error condition but the expected status
      
        if type(obj) == Job and self.valid( obj.runningJob ):
            
            # the object passed is a valid Job
                
            command = "glite-wms-job-output --json --noint --dir /tmp/ " \
                      + obj.runningJob['schedulerId']
            
            out, ret = self.ExecuteCommand( self.proxyString + command )
            
            if ret == 1 and \
                ( out.find("No output files to be retrieved") != -1 or \
                  out.find("Output files already retrieved") != -1 ) :
                # now this is the expected exit condition... 
                self.logging.debug("Purge of %s successfully" 
                                   % str(obj.runningJob['schedulerId']))
            else : 
                obj.runningJob.errors.append(out)
                
            tmp = re.search(self.pathPattern, out)
            os.system( 'rm -rf ' + tmp.group(1) )
                            
        elif type(obj) == Task :
            
            # the object passed is a Task
            
            for job in obj.jobs:
                
                if not self.valid( job.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir /tmp/ " \
                          + job.runningJob['schedulerId']
                
                out, ret = self.ExecuteCommand( self.proxyString + command )
                
                if ret == 1 and \
                    ( out.find("No output files to be retrieved") != -1 or \
                      out.find("Output files already retrieved") != -1 ) :
                    # now this is the expected exit condition... 
                    self.logging.debug("Purge of %s successfully" 
                                       % str(obj.runningJob['schedulerId']))
                else : 
                    obj.runningJob.errors.append(out)
                
                tmp = re.search(self.pathPattern, out)
                os.system( 'rm -rf ' + tmp.group(1) )

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
            
            schedIdList = ""
            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                schedIdList += " " + \
                               str( job.runningJob['schedulerId'] ).strip()
        
        command = "glite-wms-job-cancel --json --noint " + schedIdList
        
        out, ret = self.ExecuteCommand( self.proxyString + command )
        
        if ret != 0 :
            raise SchedulerError('error executing glite-wms-job-cancel', out)
        elif ret == 0 and out.find("result: success") == -1 :
            raise SchedulerError('error', out)

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
            self.delegateProxy()
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
        if type(obj) == Task :

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
                        parentIds.append( str(job.runningJob['schedulerParentId']))
                    
                count += 1
        
            if jobIds :
                formattedParentIds = ','.join(parentIds)
                formattedJobIds = ','.join(jobIds)
                
                command = 'python ' + self.commandQueryPath \
                    + 'GLiteStatusQuery.py --parentId=%s --jobId=%s' \
                        % (formattedParentIds, formattedJobIds)
                
                outJson, ret = self.ExecuteCommand( self.prefixCommandQuery + \
                                                    self.proxyString + command)
                 
                # parse JSON output
                try:
                    out = json.loads(outJson)
                    # DEBUG # print json.dumps( out,  indent=4 )
                except ValueError:
                    raise SchedulerError('error parsing JSON', out )
                    
                # Check error
                if ret != 0 or out['errors']:
                    # obj.errors doesn't exist for Task object...
                    obj.warnings.append( "Errors: " + str(out['errors']) )
                    raise SchedulerError('error executing GLiteStatusQuery', \
                                             str(out['errors']))
                
                # Refill objects...
                count = 0
                newStates = out['statusQuery']
            
                for jobId in jobIds.values() : 
                    
                    obj.jobs[jobId].runningJob['status'] = \
                                newStates[count]['status']
                    obj.jobs[jobId].runningJob['scheduledAtSite'] = \
                                newStates[count]['scheduledAtSite']
                    obj.jobs[jobId].runningJob['startTime'] = \
                                newStates[count]['startTime']
                    obj.jobs[jobId].runningJob['service'] = \
                                newStates[count]['service']
                    obj.jobs[jobId].runningJob['statusScheduler'] = \
                                newStates[count]['statusScheduler']
                    obj.jobs[jobId].runningJob['destination'] = \
                                newStates[count]['destination']
                    obj.jobs[jobId].runningJob['statusReason'] = \
                                newStates[count]['statusReason']
                    obj.jobs[jobId].runningJob['lbTimestamp'] = \
                                newStates[count]['lbTimestamp']
                    obj.jobs[jobId].runningJob['stopTime'] = \
                                newStates[count]['stopTime']
                        
                    count += 1
                  
        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    ##########################################################################

    def jobDescription (self, obj, requirements='', config='', service = ''):

        """
        retrieve scheduler specific job description
        """

        # decode obj
        return self.decode( obj, requirements='' )

        
    ##########################################################################

    def decode  ( self, obj, requirements='' ) :
        """
        prepare file for submission
        """

        if type(obj) == RunningJob or type(obj) == Job :
            return self.jdlFile ( obj, requirements ) 
        elif type(obj) == Task :
            return self.collectionJdlFile ( obj, requirements ) 


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
        isbIndex = 0

        # task input files handling:
        if task['startDirectory'] is None or task['startDirectory'][0] == '/':
            # files are stored locally, compose with 'file://'
            if task['globalSandbox'] is not None :
                for ifile in task['globalSandbox'].split(','):
                    if ifile.strip() == '' :
                        continue
                    filename = os.path.abspath( ifile )
                    globalSandbox += '"file://' + filename + '",'
                    commonFiles += "root.inputsandbox[%d]," % isbIndex
                    isbIndex += 1
        else :
            # files are elsewhere, just add their composed path
            if task['globalSandbox'] is not None :
                jdl += 'InputSandboxBaseURI = "%s";\n' % task['startDirectory']
                for ifile in task['globalSandbox'].split(','):
                    if ifile.strip() == '' :
                        continue
                    if ifile.find( 'file:/' ) == 0:
                        globalSandbox += '"' + ifile + '",'
                        
                        commonFiles += "root.inputsandbox[%d]," % isbIndex
                        isbIndex += 1
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
            # add their name in the global sandbox and put a reference
            localfiles = commonFiles
            if task['startDirectory'] is None \
                   or task['startDirectory'][0] == '/':
                # files are stored locally, compose with 'file://'
                for filePath in job['fullPathInputFiles']:
                    if filePath != '' :
                        localfiles += "root.inputsandbox[%d]," % isbIndex
                    globalSandbox += '"file://' + filePath + '",'
                    isbIndex += 1
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
        except :
            # catch a generic exception (?)
            pass

        # close jdl
        jdl += 'SignificantAttributes = {"Requirements", "Rank", "FuzzyRank"};'
        jdl += "\n]\n"

        # return values
        return jdl

    ##########################################################################
    
    def lcgInfoVo (self, tags, fqan, seList=None, blacklist=None,  
                  whitelist=None, full=False):
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
    
    ##########################################################################
    
    def lcgInfo (self, tags, vos, seList=None, blacklist=None, 
                whitelist=None, full=False):
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