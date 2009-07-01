import os,sys
import commands
import traceback
from  time import *

from ProdCommon.BossLite.Common.System import executeCommand

class Token:
    """
    basic class to handle user Token  
    """
    def __init__( self, **args ):
        self.timeout = args.get( "timeout", None )
        self.myproxyServer = args.get( "myProxySvr", '')
        self.serverDN = args.get( "serverDN", '')
        self.shareDir = args.get( "shareDir", '')
        self.userName = args.get( "userName", '')
        self.debug = args.get("debug",False)
        self.logging = args.get( "logger", logging )
        self.args = args

    def ExecuteCommand( self, command ):
        """
        _ExecuteCommand_

        Util it execute the command provided in a popen object with a timeout
        """

        return executeCommand( command, self.timeout )

        
    def registerCredential( self ):
        """
        """
        credentialDict = {} 

        credentialDict[self.getUserKerberos()]='KRB5_%s'%self.userName

        self.delegate( credentialDict )

        return 

    def getUserKerberos( self ):
        """ 
        """ 
        try: 
            kerbFile = os.path.expandvars('$KRB5CCNAME').split('FILE:')[1]
        except Exception,ex:
            msg = ('Error %s in getUserKereros search\n' %str(ex))
            if self.debug : msg += traceback.format_exc()
            raise Exception(msg)
        return kerbFile

    def delegate( self, dict ):
        """
        """
        serverName = self.args['serverName']
        for i in dict.keys():
            cmd = 'rfcp %s %s:/data/proxyCache/%s'%(i,serverName,dict[i])         

            out, ret = self.ExecuteCommand(cmd)  
            if ret != 0 :
                msg = ('Error %s in delegate while executing : %s ' % (out, cmd)) 
                raise Exception(msg)
            cmd = 'rfchmod 777  %s:/data/proxyCache/%s'%(serverName,dict[i])         

            out, ret = self.ExecuteCommand(cmd)  
            if ret != 0 :
                msg = ('Error %s in delegate while executing : %s ' % (out, cmd)) 
                raise Exception(msg)
        return 


    def getTimeLeft( self, userKerb ):
        """
        """
        expires = None
        if userKerb == None:
            userKerb = self.getUserKerberos()

        cmd = 'klist -c %s'%userKerb         

        out, ret = self.ExecuteCommand(cmd)  
        if ret != 0 :
            msg = ('Error %s in checkCredential while executing : %s ' % (out, cmd)) 
            raise Exception(msg)
        lines = out.split('\n')
        for i in range(len(lines)) :
            if lines[i].find('Expires') > 1:
                expires = lines[i+1].split('  ')[1]
        expires =  mktime(strptime(expires, '%m/%d/%y %H:%M:%S'))
        now = time()
        expires = expires - now 
        return expires


    def getSubject( self, userKerb ):
        """
        """
        expires = None
        if userKerb == None:
            userKerb = self.getUserKerberos()
        cmd = 'klist -c %s'%userKerb         

        out, ret = self.ExecuteCommand(cmd)  
        if ret != 0 :
            msg = ('Error %s in checkCredential while executing : %s ' % (out, cmd)) 
            raise Exception(msg)
        lines = out.split('\n')
        for line in lines :
            if line.find('Default principal') >= 0:
                subject = line.split(':')[1].split('@')[0]
        return subject.strip()

    def getUserName( self,userKerb ):
        """
        """
        return self.getSubject( userKerb )

 
    def destroyCredential(self, userKerb):
        """    
        """    
        if userKerb == None:
            msg = "Error no valid user kerberos to remove "
            raise Exception(msg)

        cmd = 'rm %s'%proxy

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while removing user kerberos %s"%userKerb
            raise Exception(msg)

    def ManualRenewCredential( self, proxy=None, vo='cms', group=None, role=None ):
        """
        """
        cmd = 'kinit '

        try:
            out = os.system(cmd)
            if (out>0): raise Exception("Unable to create a valid Token!\n")
        except:
            msg = "Unable to create a valid Token!\n"
            raise Exception(msg)
