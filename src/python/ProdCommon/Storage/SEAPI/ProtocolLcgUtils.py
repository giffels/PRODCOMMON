"""
Protocol that makes usage of lcg-utils to make operation with
 srm and gridftp endpoint
"""

from Protocol import Protocol
from Exceptions import *
import os

class ProtocolLcgUtils(Protocol):
    """
    implementing storage interaction with lcg-utils
    """

    def __init__(self):
        super(ProtocolLcgUtils, self).__init__()
        self.options  = " --verbose "
        self.options += " --vo=cms "
         
        env = ''
        source = self.expandEnv('RUNTIME_AREA', '/CacheEnv.sh')
        if os.path.isfile(str(source).strip()):
            env = str(source)
        vars = {\
                 'OSG_GRID': '/setup.sh', \
                 'GLITE_WMS_LOCATION': '/etc/profile.d/glite-wmsui.sh',  \
                 'GLITE_LOCATION': '/../etc/profile.d/grid-env.sh', \
#                 'GLITE_WMS_LOCATION': '/../etc/profile.d/grid-env.sh', \
                 'GRID_ENV_LOCATION': '/grid-env.sh'\
               }
        if len(env) == 0:
            for key in vars.keys():
                source = self.expandEnv(key, vars[key])
                if os.path.isfile(str(source).strip()):
                   env = str(source)
                   break;
        if len(env) > 0:
            self.fresh_env = 'unset LD_LIBRARY_PATH; unset GLITE_ENV_SET; export PATH=/usr/bin:/bin; source /etc/profile; source %s ; '%env
        else:
            raise Exception("Missing environment")

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text lookng for Exceptions
        """
        problems = []
        lines = []
        if outLines.find("\n") != -1:
            lines = outLines.split("\n")
        for line in lines:
            line = line.lower()
            if line.find("no entries for host") != -1 or\
               line.find("srm client error") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("command not found") != -1:
                raise MissingCommand("Command not found: client not " \
                                     "installed or wrong environment", \
                                     [line], outLines)
            elif line.find("user has no permission") != -1 or\
                 line.find("permission denied") != -1 and line.find("could not open lock file") == -1:
				# Messages of the form "Could not open lock file <filename>: Permission denied"
				# are non-fatal warnings relating to GLOBUS_TCP_PORT_RANGE_STATE_FILE.  Ignore them.
                raise AuthorizationException("Permission denied!", \
                                              [line], outLines)
            elif line.find("file exists") != -1:
                raise AlreadyExistsException("File already exists!", \
                                              [line], outLines)
            elif line.find("no such file or directory") != -1 or \
               line.find("error") != -1 or line.find("Failed") != -1 or \
               line.find("cacheexception") != -1 or \
               line.find("does not exist") != -1 or \
               line.find("not found") != -1 or \
               line.find("could not get storage info by path") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("unknown option") != -1 or \
                 line.find("unrecognized option") != -1 or \
                 line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                  [line], outLines)
        return problems

    def createDir(self, source, proxy = None, opt = "", tout = None):
        """
        edg-gridftp-mkdir
        """
        pass

    def copy(self, source, dest, proxy = None, opt = "", tout = None):
        """
        lcg-cp
        """
        fullSource = source.getLynk()
        fullDest = dest.getLynk()

        setProxy = ''
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "
 
        cmd = self.fresh_env + setProxy + " lcg-cp " + self.options + " " + opt + " " + \
                           fullSource + " " + fullDest
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None, opt = "", tout = None):
        """
        copy() + delete()
        """
        self.copy(source, dest, proxy, opt)
        self.delete(source, proxy, opt)

    def delete(self, source, proxy = None, opt = "", tout = None):
        """
        lcg-del
        """
        fullSource = source.getLynk()
        opt += " --nolfc"

        setProxy = ''
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "

        cmd = self.fresh_env + setProxy + "lcg-del "+ self.options +" " + opt + " " + fullSource
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy = None, opt = "", tout = None):
        """
        lcg-ls (lcg-gt)
        """
        if source.protocol in ["gsiftp-lcg"]:
            try:
                self.getTurl(source, proxy, opt)
            except OperationException:
                return False
            return True
        else:
            fullSource = source.getLynk()
            cmd = ""
            cmd += self.fresh_env
            if proxy is not None:
                cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
                self.checkUserProxy(proxy)
            cmd += "lcg-ls " + opt + " " + fullSource
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            problems = self.simpleOutputCheck(outputs)
            if exitcode != 0 or len(problems) > 0:
                if str(problems).find("no such file or directory") != -1 or \
                   str(problems).find("does not exist") != -1 or \
                   str(problems).find("not found") != -1: # and \
                   #str(problems).find("cacheexception") != -1):
                    return False
                raise OperationException("Error checking ["+source.workon+"]", \
                                         problems, outputs )
            return True

    def getFileSize(self, source, proxy = None, opt = "", tout = None):
        """
        lcg-ls
        """
        fullSource = source.getLynk()
        cmd = ""
        cmd += self.fresh_env
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        cmd += "lcg-ls -l " + opt + " "+ fullSource +" | awk '{print $5}'"
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return int(outputs)


    def getTurl(self, source, proxy = None, opt = "", tout = None):
        """
        return the gsiftp turl
        """
        fullSource = source.getLynk()
        cmd = ""
        cmd += self.fresh_env
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        cmd += "lcg-gt " + opt + " " + str(fullSource) + " gsiftp"
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0]


    def listPath(self, source, proxy = None, opt = "", tout = None):
        """
        lcg-ls (lcg-gt)
        """
        if source.protocol in ["gsiftp-lcg"]:
            raise NotImplementedException
        else:
            fullSource = source.getLynk()
            cmd = ""
            cmd += self.fresh_env
            if proxy is not None:
                cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
                self.checkUserProxy(proxy)
            cmd += "lcg-ls " + opt + " " + fullSource
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            problems = self.simpleOutputCheck(outputs)
            if exitcode != 0 or len(problems) > 0:
                if str(problems).find("such file or directory") != -1 or \
                   str(problems).find("does not exist") != -1 or \
                   str(problems).find("not found") != -1:
                    return False
                raise OperationException("Error checking ["+source.workon+"]", \
                                         problems, outputs )
            return self.getFileList(outputs, source.getFullPath())

    def getFileList(self, parsingout, startpath):
        """
        _getFileList_
        """
        filesres = []
        startpath_list=self.removeSlash(startpath)
        for line in parsingout.split("\n"):
            line_list=self.removeSlash(line)
            if ''.join(line_list).find(''.join(startpath_list)) != -1:
                filesres.append(line)
        return filesres

    def removeSlash(self,longstring):
        """ 
        remove all slashes from the given string 
        and retourn a list without empty element.      
        """    
        listEl=longstring.split('/')  
        n_empty=listEl.count('')
        for i in range(n_empty): 
            listEl.remove('')    
        return listEl
