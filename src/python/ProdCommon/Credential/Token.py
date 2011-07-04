import os,sys
import commands
import traceback
from  time import *
import tempfile
import logging

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
        self.ksuCmd = 'cd /tmp; unset LD_LIBRARY_PATH; export PATH=/usr/bin:/bin; source /etc/profile; '

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
            msg = ('Error %s in getUserKerberos search\n' %str(ex))
            msg += ('\tPlease check if $KRB5CCNAME is correctly defined.')
            if self.debug : msg += traceback.format_exc()
            raise Exception(msg)
        return kerbFile

    def delegate( self, dict ):
        """
        """
        serverName = self.args['serverName']
        proxyPath = self.args['proxyPath']

        for i in dict.keys():
            cmd = 'rfcp %s %s:%s/%s'%(i,serverName,proxyPath,dict[i])

            out, ret = self.ExecuteCommand(cmd)  
            if ret != 0 :
                msg = ('Error %s in delegate while executing : %s ' % (out, cmd)) 
                raise Exception(msg)
            cmd = 'rfchmod 760  %s:%s/%s'%(serverName,proxyPath,dict[i])
 
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

    def checkMyProxy( self , userKerb=None, Time=100, checkRetrieverRenewer=False):
        """ 
        Note The Name is Really CONFUSING... but functionality is the same as for myproxy
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
            if lines[i].find('renew until') >=1:
                expires =lines[i].split('renew until')[1].strip()
        expires =  mktime(strptime(expires, '%m/%d/%y %H:%M:%S'))
        now = time()
        timeLeft = expires - now

        minTimeLeft=int(Time)*3600 # in seconds
        valid = True

        ## if no valid proxy
        if not timeLeft :
            valid = False
        elif int(timeLeft)<minTimeLeft :
            valid = False
        return valid


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

        cmd = 'rm %s'%userKerb

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while removing user kerberos %s"%userKerb
            raise Exception(msg)

    def ManualRenewCredential( self, proxy=None, vo='cms', group=None, role=None ):
        """
        """
        cmd = 'kinit -5 -l 24h -r 240h'

        try:
            out = os.system(cmd)
            if (out>0): raise Exception("Unable to create a valid Token!\n")
        except:
            msg = "Unable to create a valid Token!\n"
            raise Exception(msg)

    def renewalMyToken(self, userKerb):
        """
        """
        if userKerb == None:
            userKerb = self.getUserKerberos()

        command =  "kinit -R -c FILE:%s" %userKerb + ";chmod 777 %s" %userKerb
        cmd = '%s\n'%command
        command,fname = self.createCommand(cmd, userKerb)
 
        out, ret = self.executeCommandWrapper( command )

        if self.ksuCmd: os.unlink( fname )
        if (ret>0): 
            raise Exception("Unable to create a valid Token!\n")


    def createCommand(self, cmd, cert):
        """
        write a ksu tmpFile
        """
        BaseCmd = self.ksuCmd +'/usr/kerberos/bin/ksu %s -k -c FILE:%s < '%(cert.split('_')[1],cert)

        tmp, fname = tempfile.mkstemp( "", "ksu_", os.getcwd() )
        os.close( tmp )
        tmpFile = open( fname, 'w')
        tmpFile.write( cmd )
        tmpFile.close()

        #redefine command
        command = BaseCmd + fname

        return command, fname


    def executeCommandWrapper(self, command ):
        """
        try to execute ksu command
        """
        out, ret = self.ExecuteCommand( command )

        tries = 0
        while (ret != 0 and tries < 5):
            out, ret = self.ExecuteCommand( command )
            tries += 1
        return out, ret

    def logonMyProxy( self, proxyCache, userDN, vo='cms', group=None, role=None):
        """
        """
        proxyFilename = os.path.join(proxyCache,"KRB5_%s"%userDN)

        return proxyFilename
