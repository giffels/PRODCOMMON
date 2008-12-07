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
        self.args = args

    def ExecuteCommand( self, command ):
        """
        _ExecuteCommand_

        Util it execute the command provided in a popen object with a timeout
        """

        return executeCommand( command, self.timeout )

        
    def registerCredential( self, command ):
        """
        """
        credentialDict = {} 
        if command == 'submit': credentialList[self.getUserToken()]='Token_%s'%self.userName

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

    def getUserToken(self):
        """
        """
        userToken = os.path.join(self.shareDir,'Token_%s'%self.userName) 

        cmd = '/afs/usr/local/etc/GetToken > ' + userToken

        out, ret =  self.ExecuteCommand(cmd)  
        if ret != 0 :
            msg = ('Error %s in getToken while executing : %s ' % (out, cmd)) 
            raise Exception(msg)
 
        return userToken

    def delegate( self, dict ):
        """
        """
        serverName = self.args['serverName']
        for i in dict.keys():
            cmd = 'rfcp %s %s:/data/proxyCache/%s'%(i,serverName,dict[i])         

            out, ret = self.ExecuteCommand(cmd)  
            if ret != 0 :
                msg = ('Error %s in getToken while executing : %s ' % (out, cmd)) 
                raise Exception(msg)
            cmd = 'rfchmod 777 '+serverName+':/data/proxyCache/%s'%os.path.basename(i)         

            out, ret = self.ExecuteCommand(cmd)  
            if ret != 0 :
                msg = ('Error %s in getToken while executing : %s ' % (out, cmd)) 
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
