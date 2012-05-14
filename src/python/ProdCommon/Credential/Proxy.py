import os,sys
import commands
import traceback
import time
import re
import logging
from ProdCommon.BossLite.Common.System import executeCommand
try:
    from hashlib import sha1
except:
    from sha import sha as sha1 

class Proxy:
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


    def getUserProxy(self):
        """
        """
        proxy = None
        if os.environ.has_key('X509_USER_PROXY'):
            proxy = os.environ['X509_USER_PROXY']
        else:
            proxy = '/tmp/x509up_u'+str(os.getuid())

        return proxy.strip()

    def getSubject(self, proxy = None):
        """
        """
        subject = None
        if proxy == None: proxy=self.getUserProxy()

        cmd = 'openssl x509 -in '+proxy+' -subject -noout'

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while checking proxy subject for %s"%proxy
            raise Exception(msg)

        subjList = []
        for s in out.split('/'):
            if 'subject' in s: continue
            if 'proxy' in s: continue
            subjList.append(s)

        subject = '/' + '/'.join(subjList)
        return subject.strip()

    def getUserName(self, proxy = None ):
        """
        """
        uName = None
        if proxy == None: proxy=self.getUserProxy()

        cmd = "voms-proxy-info -file "+proxy+" -subject"

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while extracting User Name from proxy %s"%proxy
            raise Exception(msg)

        uName = ''
        for cname in out.split('/'):
            if cname[:3] == "CN=" and cname[3:].find('proxy') == -1:
                if len(cname[3:]) > len(uName):
                    uName = cname[3:]

        return uName.strip()

    def getTimeLeft(self, proxy = None ):
        """
        """
        if proxy == None: proxy=self.getUserProxy()
        if not os.path.exists(proxy):
            return 0

        cmd = 'voms-proxy-info -file '+proxy+' -timeleft 2>/dev/null'

        timeLeftLocal,  ret = self.ExecuteCommand(cmd)

        if ret != 0 and ret != 1:
            msg = "Error while checking proxy timeleft for %s"%proxy
            raise Exception(msg)

        result = -1
        try:
            result = int(timeLeftLocal)
        except Exception:
            result = 0
        if result > 0:
            ACTimeLeftLocal = self.getVomsLife(proxy)
            if ACTimeLeftLocal > 0:
                result = self.checkLifeTimes(int(timeLeftLocal), ACTimeLeftLocal, proxy)
            else:
                result = 0
        return result

    def checkLifeTimes(self, ProxyLife, VomsLife, proxy):
        """
        """
        if abs(ProxyLife - VomsLife) > 900 :
            h=int(ProxyLife)/3600
            m=(int(ProxyLife)-h*3600)/60
            proxyLife="%d:%02d" % (h,m)
            h=int(VomsLife)/3600
            m=(int(VomsLife)-h*3600)/60
            vomsLife="%d:%02d" % (h,m)
            msg =  "proxy lifetime %s is different from voms extension lifetime%s for proxy %s\n CRAB will ask ask you create a new proxy" % (proxyLife, vomsLife, proxy)
            self.logging.info(msg)
            result = 0
        else:
            result = ProxyLife
        return result

    def getVomsLife(self, proxy):
        """
        """
        cmd = 'voms-proxy-info -file '+proxy+' -actimeleft 2>/dev/null'

        ACtimeLeftLocal,  ret = self.ExecuteCommand(cmd)

        if ret != 0 and ret != 1:
            msg = "Error while checking proxy actimeleft for %s"%proxy
            raise Exception(msg)

        result = -1
        try:
            result = int(ACtimeLeftLocal)
        except Exception:
            msg  =  "voms extension lifetime for proxy %s is 0 \n"%proxy
            msg +=  "\tCRAB will ask ask you create a new proxy"
            self.logging.info(msg)
            result = 0
        return result

    def renewCredential( self, proxy=None ):
        """
        """
        if proxy == None: proxy=self.getUserProxy()
        # check
        if not self.checkCredential():
            # ask for proxy delegation
            # using myproxy
            pass
        return

    def checkAttribute( self, proxy=None, vo='cms', group=None, role=None):
        """
        """
        valid = True
        if proxy == None: proxy=self.getUserProxy()

        ## check first attribute
        cmd = 'export X509_USER_PROXY=%s; voms-proxy-info -fqan 2>/dev/null | head -1'%proxy

        reg="/%s/"%vo
        if group:
            reg+=group
            if role: reg+="/Role=%s"%role
        else:
            if role: reg+="Role=%s"%role

        att, ret = self.ExecuteCommand(cmd)

        if ret != 0 :
            msg = "Error while checking attribute for %s"%proxy
            raise Exception(msg)

       ## you always have at least  /cms/Role=NULL/Capability=NULL
        if not re.compile(r"^"+reg).search(att):
            self.logging.info("Wrong VO group/role.")
            valid = False
        return valid

    def ManualRenewCredential( self, proxy=None, vo='cms', group=None, role=None ):
        """
        """
        cmd = 'voms-proxy-init -voms %s'%vo

        if group:
            cmd += ':/'+vo+'/'+group
            if role: cmd += '/Role='+role
        else:
            if role: cmd += ':/'+vo+'/Role='+role

        cmd += ' -valid 192:00'
        try:
            out = os.system(cmd)
            if (out>0): raise Exception("Unable to create a valid proxy!\n")
        except:
            msg = "Unable to create a valid proxy with command:\n%s\n"%cmd
            raise Exception(msg)

    def destroyCredential(self, proxy):
        """
        """
        if proxy == None:
            msg = "Error no valid proxy to remove "
            raise Exception(msg)
        cmd = '[ -e %s ] && rm %s'%(proxy, proxy)

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while removing proxy %s"%proxy
            raise Exception(msg)

        return

    def checkMyProxy( self , proxy=None, Time=4, checkRetrieverRenewer=False):
        """
        """
        if proxy == None: proxy=self.getUserProxy()
        ## check the myproxy server
        valid = True

        #cmd = 'export X509_USER_PROXY=%s; myproxy-info -d -s %s 2>/dev/null'%(proxy,self.myproxyServer)
        cmd = 'myproxy-info -d -s %s 2>/dev/null'%(self.myproxyServer)

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 and ret != 1 :
            msg = "Error while checking myproxy timeleft for %s from %s: %s"%(proxy, self.myproxyServer, out)
            raise Exception(msg)

        if not out:
            self.logging.info('No credential delegated to myproxy server %s .'%self.myproxyServer)
            valid = False
        else:

            ## minimum time: 4 days
            minTime = int(Time) * 24 * 3600
            ## regex to extract the right information
            timeleftList = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)").findall(out)
            timeleft, hours, minutes, seconds = 0, 0, 0, 0 

            ## the first time refers to the flat user proxy, the other ones are related to the server credential name
            try:
                hours, minutes, seconds = timeleftList[0]
                timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
            except Exception, e:
                self.logging.info('Error extracting timeleft from proxy')
                self.logging.debug( str(e) )
                valid = False
            if timeleft < minTime:
                self.logging.info('Your proxy will expire in:\n\t%s hours %s minutes %s seconds\n'%(hours,minutes,seconds))
                valid = False

            # check the timeleft for the required server
            if checkRetrieverRenewer and len(self.serverDN.strip()) > 0:
                serverCredName = sha1(self.serverDN).hexdigest()
                credNameList = re.compile(" name: (?P<CN>.*)").findall(out)
                credTimeleftList = timeleftList[1:]

                # check if the server credential exists
                if serverCredName not in credNameList :
                    self.logging.info('Your proxy lacks of retrieval and renewal policies for the requested server.')
                    self.logging.info('Renew your myproxy credentials.')
                    valid = False
                    return valid

                try:
                    hours, minutes, seconds = credTimeleftList[ credNameList.index(serverCredName) ]
                    timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
                except Exception, e:
                    self.logging.info('Error extracting timeleft from credential name')
                    self.logging.debug( str(e) )
                    valid = False
                if timeleft < minTime:
                    logMsg  = 'Your credential for the required server will expire in:\n\t'
                    logMsg += '%s hours %s minutes %s seconds\n'%(hours,minutes,seconds)
                    self.logging.info(logMsg)
                    valid = False

                # clean up expired credentials for other servers
                cleanCredCmdList = []
                for credIdx in xrange(len(credNameList)):
                    hours, minutes, seconds = credTimeleftList[ credIdx ]
                    timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
                    if timeleft == 0:
                        cleanupCmd = "myproxy-destroy -d -k %s"%(credNameList[credIdx]) 
                        cleanCredCmdList.append( cleanupCmd )  
                    pass

                cleanCredCmd = " && ".join(cleanCredCmdList)
                if len(cleanCredCmd)>0:
                    self.logging.debug('Removing expired credentials: %s'%cleanCredCmd) 
                    try:
                        out, ret = self.ExecuteCommand( cleanCredCmd )
                    except:
                        self.logging.debug('Error in cleaning expired credentials. Ignore and go ahead.')
                        pass
                   
        return valid

    def ManualRenewMyProxy( self ):
        """
        """
        cmd = 'myproxy-init -d -n -s %s'%self.myproxyServer

        if len( self.serverDN.strip() ) > 0:
            credName = sha1(self.serverDN).hexdigest()
            cmd += ' -x -R \'%s\' -Z \'%s\' -k %s -t 168:00 '%(self.serverDN, self.serverDN, credName)

        out = os.system(cmd)
        self.logging.debug('MyProxy delegation:\n%s'%cmd)
        if (out>0):
            raise Exception("Unable to delegate the proxy to myproxyserver with command \n" % cmd )
        return

    def logonMyProxy( self, proxyCache, userDN, vo='cms', group=None, role=None):
        """
        """

        # myproxy-logon -d -n -s $MYPROXY_SERVER -o <outputFile> -l <userDN> -k <credName>

        # compose the VO attriutes
        voAttr = vo
        if group:
            voAttr += ':/'+vo+'/'+group
            if role: voAttr += '/Role='+role
        else:
            if role: voAttr += ':/'+vo+'/Role='+role

        # get the credential name for this retriever
        credName = sha1( self.getSubject('$HOME/.globus/hostcert.pem') ).hexdigest()

        # compose the delegation or renewal commands with the regeneration of Voms extensions
        cmdList = []
        cmdList.append('unset X509_USER_CERT X509_USER_KEY')
        cmdList.append('&& env')
        cmdList.append('X509_USER_CERT=$HOME/.globus/hostcert.pem')
        cmdList.append('X509_USER_KEY=$HOME/.globus/hostkey.pem')

        ## get a new delegated proxy
        proxyFilename= os.path.join(proxyCache, sha1(userDN + voAttr).hexdigest() )

        cmdList.append('myproxy-logon -d -n -s %s -o %s -l \"%s\" -k %s -t 168:00'%\
            (self.myproxyServer, proxyFilename, userDN, credName) )

        cmd = ' '.join(cmdList)
        msg, out = self.ExecuteCommand(cmd)

        if (out>0):
            self.logging.info('MyProxy logon - retrieval FAILED - command issued:\n%s'%cmd)
            self.logging.info('MyProxy logon - command output:\n%s'%msg)
            raise Exception("Unable to retrieve delegated proxy for user DN %s! Exit code:%s"%(userDN, out) )
        else:
            self.logging.debug('MyProxy logon - retrieval OK :\n%s'%cmd)

        self.vomsExtensionRenewal(proxyFilename, voAttr)

        return proxyFilename

    def renewalMyProxy(self, proxyFilename):
        """
        """

        # get vo, group and role from the current certificate
        cmd = 'env X509_USER_PROXY=%s voms-proxy-info -vo 2>/dev/null | head -1'%proxyFilename
        att, ret = self.ExecuteCommand(cmd)
        if ret != 0:
            raise Exception("Unable to get VO for proxy %s! Exit code:%s"%(proxyFilename, ret) )
        vo = att.replace('\n','')

        # at least /cms/Role=NULL/Capability=NULL
        cmd = 'env X509_USER_PROXY=%s voms-proxy-info -fqan 2>/dev/null | head -1'%proxyFilename
        att, ret = self.ExecuteCommand(cmd)
        if ret != 0:
            raise Exception("Unable to get FQAN for proxy %s! Exit code:%s"%(proxyFilename, ret) )

        # prepare the attributes
        att = att.split('\n')[0]
        att = att.replace('/Role=NULL','')
        att = att.replace('/Capability=NULL','')

        voAttr = vo + ':' + att

        # get the credential name for this renewer
        credName = sha1( self.getSubject('$HOME/.globus/hostcert.pem') ).hexdigest()

        # renew the certificate
        # compose the delegation or renewal commands with the regeneration of Voms extensions
        cmdList = []
        cmdList.append('unset X509_USER_CERT X509_USER_KEY')
        cmdList.append('&& env')
        cmdList.append('X509_USER_CERT=$HOME/.globus/hostcert.pem')
        cmdList.append('X509_USER_KEY=$HOME/.globus/hostkey.pem')

        ## refresh an existing proxy
        cmdList.append('myproxy-logon -d -n -s %s -a %s -o %s -k %s -t 168:00'%\
            (self.myproxyServer, proxyFilename, proxyFilename, credName) )

        cmd = ' '.join(cmdList)
        msg, out = self.ExecuteCommand(cmd)
        if (out>0):
            self.logging.info('MyProxy logon - retrieval FAILED - command issued:\n%s'%cmd)
            self.logging.info('MyProxy logon - command output:\n%s'%msg)
            raise Exception("Unable to retrieve proxy for renewal: %s! Exit code:%s"%(proxyFilename, out) )
        else:
            self.logging.debug('MyProxy renewal - logon OK :\n%s'%cmd)

        self.vomsExtensionRenewal(proxyFilename, voAttr)

        return

    def vomsExtensionRenewal(self, proxy, voAttr='cms'):
        ## get validity time for retrieved flat proxy
        cmd = 'grid-proxy-info -file '+proxy+' -timeleft 2>/dev/null'

        timeLeft,  ret = self.ExecuteCommand(cmd)
        if ret != 0 and ret != 1:
            raise Exception("Error while checking retrieved proxy timeleft for %s"%proxy )

        try:
            timeLeft = int(timeLeft) - 60
        except Exception:
            timeLeft = 0

        self.logging.debug( 'Timeleft for retrieved proxy: (exit code %s) %s'%(ret, timeLeft) )

        if timeLeft <= 0:
            # fake value, it would fail in any case
            vomsValid = "12:00"
        else:
            vomsValid = "%d:%02d"%( timeLeft/3600, (timeLeft-(timeLeft/3600)*3600)/60 )

        self.logging.debug( 'Requested voms validity: %s'%vomsValid )

        ## set environ and add voms extensions
        cmdList = []
        cmdList.append('env')
        cmdList.append('X509_USER_CERT=%s'%proxy)
        cmdList.append('X509_USER_KEY=%s'%proxy)
        cmdList.append('voms-proxy-init -noregen -voms %s -cert %s -key %s -out %s -bits 1024 -valid %s'%\
             (voAttr, proxy, proxy, proxy, vomsValid) )

        cmd = ' '.join(cmdList)
        msg, out = self.ExecuteCommand(cmd)
        if (out>0):
            self.logging.info('Voms extension:\n%s'%cmd)
            self.logging.info('Voms extension result:\n%s'%msg)
            raise Exception("Unable to renew proxy voms extension: %s! Exit code:%s"%(proxy, out) )
        else:
            self.logging.debug('Voms extension:\n%s'%cmd)

        return

