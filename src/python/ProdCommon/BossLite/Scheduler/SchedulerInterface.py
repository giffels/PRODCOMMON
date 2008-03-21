#!/usr/bin/env python
"""
_SchedulerInterface_

"""

from ProdCommon.BossLite.Common.Exceptions import SchedulerError
#from subprocess import Popen, PIPE, STDOUT
from os import popen4
from os import getuid

__version__ = "$Id: SchedulerInterface.py,v 1.7 2008/03/21 14:19:36 gcodispo Exp $"
__revision__ = "$Revision: 1.7 $"

class SchedulerInterface(object):
    """
    Upper layer for scheduler interaction
    
    """
 
    def __init__(self, userProxy = ''):
        """
        initialization
        """

        self.cert = userProxy
        self.checkUserProxy( self.cert )

    ##########################################################################

    def ExecuteCommand( self, command, timeout = 600 ):
        """
        _ExecuteCommand_
        
        Util it execute the command provided in a popen object with a timeout
        
        """

        #p = Popen( command, shell=True,
        #           stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True )
        #msg = p.stdout.read()

        pin, pout = popen4( command )
        msg = pout.read()
        return msg

    ##########################################################################

    def checkUserProxy( self, cert='' ):
        """
        Retrieve the user proxy for the task
        If the proxy is valid pass, otherwise raise an axception
        """

        command = 'voms-proxy-info'

        if cert != '' :
            command += ' --file ' + cert

        output = self.ExecuteCommand( command )

        try:
            output = output.split("timeleft  :")[1].strip()
        except IndexError:
            raise SchedulerError("Missing Proxy", "Missing Proxy")
    
        if output == "0:00:00":
            raise SchedulerError("Proxy Expired", "Proxy Expired")
    
    ##########################################################################

    def jobDescription ( self, obj, requirements='', config='', service = '' ):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        raise NotImplementedError
    
    ##########################################################################

    def decode ( self, obj, requirements='' ) :
        """
        prepare scheduler specific job description

        used by self.submit(), return everithing is needed
        for the actual submission
        
        """
        raise NotImplementedError
    
    ##########################################################################

    def submit ( self, obj, requirements='', config='', service = '' ) :
        """
        set up submission parameters and submit
        uses self.decode()

        return jobAttributes, bulkId, service

        - jobAttributs is a map of the format
              jobAttributes[ 'name' : 'schedulerId' ]
        - bulkId is an eventual bulk submission identifier
        - service is a endpoit to connect withs (such as the WMS)
        """
        raise NotImplementedError

    ##########################################################################
    
    def query(self, schedIdList, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        It may use single 'node' scheduler id or bulk id for association
        
        return jobAttributes

        where jobAttributes is a map of the format:
           jobAttributes[ schedId :
                                    [ key : val ]
                        ]
           where key can be any parameter of the Job object and at least status
                        
        """
        raise NotImplementedError

    ##########################################################################
    
    def getOutput( self, schedIdList, outdir, service ):
        """
        retrieve output or just put it in the destination directory

        does not return
        """
        raise NotImplementedError


    ##########################################################################

    def kill( self, schedIdList, service ):
        """
        kill the job instance

        does not return
        """
        raise NotImplementedError

    ##########################################################################

    def postMortem ( self, schedIdList, outfile, service ) :
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """
        raise NotImplementedError

    ##########################################################################

    def purgeService( self, schedIdList ):
        """
        purge the service used by the scheduler from job files
        not available for every scheduler

        does not return
        """
        raise NotImplementedError

    ##########################################################################

    def matchResources( self, obj, requirements='', config='', service='' ):
        """
        perform a resources discovery
        returns a list of resulting sites
        """
        raise NotImplementedError

    ##########################################################################

    def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """
    
        celist = []
    
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
                if ce.find( "blah" ) == -1 and ce not in blacklist :
                    if whitelist is not None and ce in whitelist :
                        celist.append( ce )
            
            return celist
        
        for se in seList :
            singleComm = command + " lcg-info --vo " + vo + \
                         " --list-ce --query " + \
                         "\'" + query + ",CloseSE="+ se + "\' --sed"
            out = self.ExecuteCommand( singleComm )
            out = out.split()

            for ce in out :
                if ce.find( "blah" ) == -1 and ce not in blacklist :
                    if whitelist is not None and ce in whitelist :
                        celist.append( ce )

        return celist
