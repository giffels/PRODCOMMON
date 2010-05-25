import sys
import commands
import traceback
import time

class CredentialAPI:
    def __init__( self, args ):

        self.credential = args.get( "credential", '')
        self.pInfos = {}
 
        try:
            module =  __import__(
                self.credential, globals(), locals(), [self.credential]
                )
            credClass = vars(module)[self.credential]
            self.credObj = credClass( **args )
        except KeyError, e:
            msg = 'Credential interface' + self.credential + 'not found'
            raise msg, str(e)
        except Exception, e:
            raise e.__class__.__name__, str(e)


    def getCredential( self ):
        """
        """

        #self.credObj
 
        return
      
    def checkCredential( self, credential=None, Time=10 ):
        """
        """
        minTimeLeft=int(Time)*3600 # in seconds
        valid = True

        timeLeftLocal = self.getTimeLeft(credential)

        ## if no valid proxy
        if not timeLeftLocal :
            valid = False
        elif int(timeLeftLocal)<minTimeLeft :
            valid = False
        return valid

    def ManualRenewCredential(self, credential=None, vo='cms', group=None, role=None):
        """   
        """   
        try:    
            self.credObj.ManualRenewCredential(credential,vo,group,role)
        except Exception, ex:
            raise Exception( str(ex))

    def registerCredential( self ):
        """
        """
        try:    
            self.credObj.registerCredential()
        except Exception, ex:
            raise Exception( str(ex))
        return

    def getSubject(self, credential=None):
        """   
        """   
        sub = ''   
        try: 
            sub = self.credObj.getSubject(credential)
        except Exception, ex:
            raise Exception(str(ex))
        return sub

    def getUserName(self, credential=None):
        """   
        """   
        uName = ''   
        try: 
            uName = self.credObj.getUserName(credential)
        except Exception, ex:
            raise Exception(str(ex))
        return uName

    def getTimeLeft(self, credential=None):
        """   
        """
        timeleft = None   
        try: 
            timeleft = self.credObj.getTimeLeft(credential)
        except Exception, ex:
            raise Exception(str(ex))
        return timeleft

    def destroyCredential(self, credential = None):
        """   
        """   
        try:  
            self.credObj.destroyCredential( credential )
        except Exception, ex:
            raise Exception(str(ex))
        

### Special stuff For Proxy
    def checkMyProxy(self, credential=None, Time=4, checkRetrieverRenewer=False ):
        """   
        """   
        valid = None
        try:  
            valid = self.credObj.checkMyProxy(credential,Time,checkRetrieverRenewer)
        except Exception, ex:
            raise Exception(str(ex))
         
        return valid

         
    def checkAttribute(self, credential=None, vo='cms', group=None, role=None ):
        """   
        """   
        valid = None
        try:  
            valid = self.credObj.checkAttribute(credential,vo,group,role)
        except Exception, ex:
            raise Exception(str(ex))
         
        return valid

    def ManualRenewMyProxy(self, credential=None):
        """   
        """   
        try:    
            self.credObj.ManualRenewMyProxy()
        except Exception, ex:
            raise Exception( str(ex))

    def logonMyProxy(self, proxyCache, userDN, vo='cms', group=None, role=None) :
        """
        """
        CredentialFileName = None
        try:
            CredentialFileName = self.credObj.logonMyProxy(proxyCache, userDN, vo, group, role)
        except Exception, ex:
            raise Exception( str(ex) )
        return CredentialFileName 

    def renewalMyProxy(self, proxyFile) :
        """
        """
        try:
            self.credObj.renewalMyProxy(proxyFile)
        except Exception, ex:
            raise Exception( str(ex) )


    def renewalMyToken(self, proxyFile) :
        """
        """
        try:
            self.credObj.renewalMyToken(proxyFile)
        except Exception, ex:
            raise Exception( str(ex) )

