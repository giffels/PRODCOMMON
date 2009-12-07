#!/usr/bin/env python
"""
basic glite CLI interaction class
"""

__revision__ = ""
__version__ = ""
__author__ = "filippo.spiga@disco.unimib.it"

import os
import tempfile
import re
import simplejson as json

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class bossLiteJsonDecoder(json.JSONDecoder):
    """
    Override JSON decode
    """
    
    def decode (self, jsonString):
        """
        basic class to decode json output
        """
        
        jsonString = jsonString.replace( '\n' , ',' )
        p = re.compile('\{,[\s]*([a-zA-Z0-9_\-])')
        jsonString = p.sub(r'{ "\1', jsonString[:-1] )
        p = re.compile(':[\s]([a-zA-Z_\-])')
        jsonString = p.sub(r'":"\1', jsonString )
        p = re.compile('[\s]*([a-zA-Z0-9_\-]*),[\s]*([a-zA-Z0-9_\-]*)"')
        jsonString = p.sub(r'\1","\2"', jsonString )
        p = re.compile('[\s]*([a-zA-Z0-9_\-]*),[\s]*([a-zA-Z0-9_\-]*):')
        jsonString = p.sub(r'\1","\2":', jsonString )
        p = re.compile(',[\s]*}(?!"[\s]*[a-zA-Z0-9_\-]*)')
        jsonString =  p.sub(r'}', jsonString)
        p = re.compile('([a-zA-Z0-9_\-])}')
        jsonString =  p.sub(r'\1"}', jsonString)

        parsedJson = json.loads(jsonString)
        
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
        self.proxyString = ''
        self.envProxy = os.environ.get("X509_USER_PROXY",'')
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
        if os.environ.get('CRABPRODCOMMONPYTHON') :
            self.commandQueryPath = os.environ.get('CRABPRODCOMMONPYTHON') + \
                                            '/ProdCommon/BossLite/Scheduler/'
        elif os.environ.get('PRODCOMMON_ROOT') :
            self.commandQueryPath = os.environ.get('PRODCOMMON_ROOT') + \
                                        '/lib/ProdCommon/BossLite/Scheduler/'
        else :
            # something went wrong... fake path (only for test purposes)
            self.commandQueryPath = '/afs/cern.ch/user/s/spigafi/scratch0/workspace/PRODCOMMON/lib/ProdCommon/BossLite/Scheduler/'
        
        glite_location = os.environ.get('GLITE_LOCATION').split('glite')[0]
        glite_ui= '%s/etc/profile.d/grid-env.sh ' % glite_location
        self.prefixCommandQuery = 'unset LD_LIBRARY_PATH; export PATH=/usr/bin:/bin; source /etc/profile; source %s ;export PYTHONPATH=${PYTHONPATH}:${GLITE_LOCATION}/lib64; ' % glite_ui

        # Raise an error if UI is old than 3.2 ...
        version, ret = self.ExecuteCommand( 'glite-version' )
        version = version.strip()
        if version.find( '3.2' ) != 0 :
            raise SchedulerError( 'SchedulerGLite is allowed on UI >3.2' )
            

    ##########################################################################

    def delegateWMSProxy( self, wms ):
        """
        delegate proxy to a wms
        """

        command = "glite-wms-job-delegate-proxy -d " + self.delegationId \
                  + " --endpoint " + wms
        msg, ret = self.ExecuteCommand( self.proxyString + command )

        if ret != 0 or msg.find("Error -") >= 0 :
            self.logging.warning( "Warning : %s" % msg )

    ##########################################################################

    def delegateProxy( self ):
        """
        delegate proxy to _all_ wms
        """

        command = "glite-wms-job-delegate-proxy -d " + self.delegationId

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
        
        myJSONDecoder = bossLiteJsonDecoder()

        try:
            jOut = myJSONDecoder.decode(out)
        except ValueError:
            raise SchedulerError('error parsing JSON output',  out)
             
        os.unlink( fname )

        returnMap = {}
        if type(obj) == Task:
            self.logging.info("Your job identifier is: %s" % jOut['parent'])
            
            for child in jOut['children'].keys() :
                returnMap[str(child.replace('NodeName_', '', 1))] = \
                                                str(jOut['children'][child])
            
            return returnMap, str(jOut['parent']), str(jOut['endpoint']) 
        elif type(obj) == Job:
            # usually we submit collections.....
            self.logging.info("Your job identifier is: %s" % jOut['jobid'])
            
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

        if type(obj) == Job and self.valid( obj.runningJob ):
            # the object passed is a valid Job
                
            command = "glite-wms-job-output --json --noint --dir " + \
                      outdir + " " + job.runningJob['schedulerId']
            
            out, ret = self.ExecuteCommand( self.proxyString + command )
            
            if ret == 1 : 
                # One or more output files have not been retrieved
                if out.find("No output files to be retrieved") != -1:
                    obj.runningJob.warning.append("No output files")
                
                # Output files already retrieved
                if out.find("Output files already retrieved") != -1 : 
                    obj.runningJob.warning.append("Already retrieved")
                
                # Output not yet ready
                if out.find("Output not yet Ready") != -1 :
                    obj.runningJob.errors.append("Not yet Ready")
            
            # Excluding all the previous cases however something went wrong....
            if ret == 0 and out.find("result: success") == -1 :
                obj.runningJob.errors.append( out )   
            
            if obj.runningJob.errors is None or obj.runningJob.errors == [] :
                self.logging.debug("Output of %s successfully retrieved" 
                                        % str(job.runningJob['schedulerId']))

        elif type(obj) == Task :
            # the object passed is a Task
            
            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir " + \
                          outdir + " " + job.runningJob['schedulerId']
                
                out, ret = self.ExecuteCommand( self.proxyString + command )
                
                if ret == 1 : 
                    # One or more output files have not been retrieved
                    if out.find("No output files to be retrieved") != -1 :
                        job.runningJob.warning.append("No output files")
                    
                    # Output files already retrieved
                    if out.find("Output files already retrieved") != -1 : 
                        job.runningJob.warning.append("Already retrieved")
                    
                    # Output not yet ready
                    if out.find("Output not yet Ready") != -1 :
                        job.runningJob.errors.append("Not yet Ready")
                
                # Excluding all the previous cases but something went wrong
                if ret == 0 and out.find("result: success") == -1 :
                    job.runningJob.errors.append(out)
                
                if job.runningJob.errors is None or job.runningJob.errors == [] :
                    self.logging.debug("Output of %s successfully retrieved" 
                                        % str(job.runningJob['schedulerId']))
                    
        

    ##########################################################################
    def purgeService( self, obj ):
        """
        Purge job (even bulk) from wms
        """

        # Implement as getOutput where the "No output files ..."
        # is not an error condition but the expected status

        # REQUIRE MODIFICATION, not completely implemented yet
        
        if type(obj) == Job and self.valid( obj.runningJob ):
            # the object passed is a valid Job
                
            command = "glite-wms-job-output --json --noint --dir " \
                      + outdir + " " + job.runningJob['schedulerId']
            
            out, ret = self.ExecuteCommand( self.proxyString + command )
            
            if ret == 1 : 
                # One or more output files have not been retrieved
                if out.find("No output files to be retrieved") != -1 :
                    obj.runningJob.warning.append("No output files")
                
                # Output files already retrieved
                if out.find("Output files already retrieved") != -1 : 
                    obj.runningJob.warning.append("Already retrieved")
                
                # Output not yet ready
                if out.find("Output not yet Ready") != -1 :
                    obj.runningJob.errors.append("Not yet Ready")
            
            # Excluding all the previous but however something went wrong
            if ret == 0 and out.find("result: success") == -1 :
                obj.runningJob.errors.append( out )   
            
            if obj.runningJob.errors is None or obj.runningJob.errors == [] :
                self.logging.debug("Output of %s successfully retrieved" 
                                    % str(job.runningJob['schedulerId']))

        elif type(obj) == Task :
            # the object passed is a Task
            
            for job in obj.jobs:
                if not self.valid( job.runningJob ):
                    continue
                
                command = "glite-wms-job-output --json --noint --dir " \
                          + outdir + " " + job.runningJob['schedulerId']
                
                out, ret = self.ExecuteCommand( self.proxyString + command )
                
                if ret == 1 : 
                    # One or more output files have not been retrieved
                    if out.find("No output files to be retrieved") != -1 :
                        job.runningJob.warning.append("No output files")
                    
                    # Output files already retrieved
                    if out.find("Output files already retrieved") != -1 : 
                        job.runningJob.warning.append("Already retrieved")
                    
                    # Output not yet ready
                    if out.find("Output not yet Ready") != -1 :
                        job.runningJob.errors.append("Not yet Ready")
                
                # Excluding all the previous cases but something went wrong
                if ret == 0 and out.find("result: success") == -1 :
                    job.runningJob.errors.append( out )
                
                if job.runningJob.errors is None or job.runningJob.errors == [] :
                    self.logging.debug("Output of %s successfully retrieved" 
                                        % str(job.runningJob['schedulerId']))
       

        
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
        
        command = self.prefixCommandQuery + self.proxyString
        
        # the object passed is a Task:
        #   check whether parent id are provided, make a list of ids
        #     and check the status
        if type(obj) == Task :

            # query performed through single job ids
            if objType == 'node' :

                # not implemented yet ...
                raise NotImplementedError

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

                formattedParentIds = ','.join(parentIds)
                formattedJobIds = ','.join(jobIds)
                
                command = command + 'python ' + self.commandQueryPath \
                    + 'GLiteStatusQuery.py --parentId=%s --jobId=%s' \
                        % (formattedParentIds, formattedJobIds)
                    
                outJson, ret = self.ExecuteCommand( command )
                
                # Check error
                if ret !=0 :
                    raise SchedulerError('error executing GLiteStatusQuery', outJson)
                else : 
                    # parse JSON output
                    try:
                        out = json.loads(outJson)
                    except ValueError:
                        raise SchedulerError('error', out )
                    
                    if out['errors']:
                        raise  SchedulerError('Errors during states retrieving', out['errors'] )
                
                # Refill objects...
                count = 0
                newStates = out['statusQuery']
                
                for id in jobIds.values() : 
                    
                    # probably this check is useless...
                    if self.valid( job.runningJob ) :
                        obj.jobs[id].runningJob['status'] = \
                                    newStates[count]['status']
                        obj.jobs[id].runningJob['scheduledAtSite'] = \
                                    newStates[count]['scheduledAtSite']
                        obj.jobs[id].runningJob['startTime'] = \
                                    newStates[count]['startTime']
                        obj.jobs[id].runningJob['service'] = \
                                    newStates[count]['service']
                        obj.jobs[id].runningJob['statusScheduler'] = \
                                    newStates[count]['statusScheduler']
                        obj.jobs[id].runningJob['destination'] = \
                                    newStates[count]['destination']
                        obj.jobs[id].runningJob['statusReason'] = \
                                    newStates[count]['statusReason']
                        obj.jobs[id].runningJob['lbTimestamp'] = \
                                    newStates[count]['lbTimestamp']
                        obj.jobs[id].runningJob['stopTime'] = \
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

