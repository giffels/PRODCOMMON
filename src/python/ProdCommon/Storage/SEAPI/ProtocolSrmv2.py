"""
Class interfacing with srm version 2 end point
"""

from Protocol import Protocol
from Exceptions import *
import string,os

class ProtocolSrmv2(Protocol):
    """
    implementing the srm protocol version 2.*
    """

    def __init__(self):
        super(ProtocolSrmv2, self).__init__()
        self._options = " -2 -debug=true -protocols=gsiftp,http " 
        # check for java version

        ### FEDE ###
        #if not os.environ.has_key('_JAVA_OPTIONS'):
        os.environ['_JAVA_OPTIONS'] = '-Xms128m -Xmx512m'
        ############

        cmd = "java -version"
        try: 
            exitcode, outputs = self.executeCommand(cmd)
        except Exception, ex:
            raise MissingCommand("Missing java command.", \
                                     [] , cmd)
        javaLink='https://twiki.cern.ch/twiki/bin/view/CMS/CheckUserJava'
        for line in outputs.split("\n"):
            line = line.lower()
            if line.find("version") != -1 and line.find("1.4") != -1:
                msg= ('%s is to old for srm needs. Please update it.\n\tFor further infos see: %s '%(str(line),javaLink))
                raise Exception(msg)

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text looking for Exceptions
        """
        problems = []
        lines = []
        if outLines.find("\n") != -1:
            lines = outLines.split("\n")
        for line in lines:
            line = line.lower()
            if line.find("exception") != -1 or \
               line.find("does not exist") != -1 or \
               line.find("srm client error") != -1 or \
               line.find("failed in some way or another") != -1 or \
               line.find("srm_failure") != -1:
                   cacheP = line.split(":")[-1]
                   if cacheP not in problems:
                       problems.append(cacheP)
            if line.find("couldn't getgsscredential") != -1:
                cacheP = line
                if cacheP not in problems:
                    problems.append(cacheP)
            if line.find("unknownhostexception") != -1 or \
               line.find("no entries for host") != -1: # or \
                 raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("srm_authorization_failure") != -1 or \
               line.find("permission denied") != -1:
                 raise AuthorizationException("Permission denied", [line], outLines)
            elif line.find("connection timed out") != -1:
                 raise OperationException("Connection timed out", [line], outLines)
            elif line.find("already exists") != -1 or \
               line.find("srm_duplication_error") != -1:
                 raise AlreadyExistsException("File already exists!", \
                                              [line], outLines)
            elif line.find("unrecognized option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)
            elif line.find("command not found") != -1:
                raise MissingCommand("Command not found: client not " \
                                     "installed or wrong environment", \
                                     [line], outLines)
        return problems

    def copy(self, source, dest, proxy = None, opt = "", tout = None):
        """
        srmcp
        """
        fullSource = "file:///" + str(source.workon)
        fullDest = "file:///" + str(dest.workon)
        if source.protocol != 'local':
            fullSource = source.getLynk()
        if dest.protocol != 'local':
            fullDest = dest.getLynk()

        opt += " %s --delegate=false "%self._options
        ### FEDE ### 
        opt += " -storagetype=permanent "
        ############

        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)
        
        cmd = "srmcp " + opt +" "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0: #or len(problems) > 0: #if exit code = 0 => skip
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None, opt = "", tout = None):
        """
        with srmmv "source and destination have to have same URL type"
         => copy and delete
        """
        if self.checkExists(dest, proxy, opt):
            problems = ["destination file already existing", dest.workon]
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems)
        self.copy(source, dest, proxy, opt)
        if self.checkExists(dest, proxy, opt):
            self.delete(source, proxy)
        else:
            raise TransferException("Error deleting [" +source.workon+ "]", \
                                     ["Uknown Problem"] )

    def deleteRec(self, source, proxy, opt = "", tout = None):
        self.delete(source, proxy, opt)

    def delete(self, source, proxy = None, opt = "", tout = None):
        """
        srmrm
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmrm " +opt +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def createDir(self, source, proxy = None, opt = "", tout = None):
        """
        srmmkdir
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)
        if self.checkDirExists(fullSource , opt = "", tout = None) is False:
            elements = fullSource.split('/')
            elements.reverse()
            if '' in elements: elements.remove('') 
            toCreate = []
            for ele in elements:
                toCreate.append(ele)
                fullSource_tmp = fullSource.split('/')
                if '' in fullSource_tmp[-1:] : fullSource_tmp = fullSource_tmp[:-1] 
                fullSource = string.join(fullSource_tmp[:-1],'/')

                if fullSource != "srm:/":
                    if self.checkDirExists(fullSource, opt = "", tout = None ) is True: break
                else:
                    break
            toCreate.reverse()   
            for i in toCreate:
                fullSource = fullSource+'/'+i
                cmd = "srmmkdir " +opt +" "+ fullSource
                exitcode, outputs = self.executeCommand(cmd, timeout = tout)
           
                ### simple output parsing ###
                problems = self.simpleOutputCheck(outputs)
                if exitcode != 0 or len(problems) > 0:
                    raise OperationException("Error creating [" +source.workon+ "]", \
                                              problems, outputs )

    def checkPermission(self, source, proxy = None, opt = "", tout = None):
        """
        return file/dir permission
        """
        return int(self.listFile(source, proxy, opt)[3])

    def getFileSize(self, source, proxy = None, opt = "", tout = None):
        """
        file size
        """
        ##size, owner, group, permMode = self.listFile(filePath, SEhost, port)
        size = self.listFile(source, proxy, opt)[0]
        return int(size)

    def listFile(self, source, proxy = None, opt = "", tout = None):
        """
        srmls

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmls " + opt +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )

        ### need to parse the output of the commands ###
        size = 0
        # for each listed file
        for line in outputs.split("\n"):
            # get correct lines
            if line.find(source.workon) != -1 and line.find("surl[0]=") == -1:
                values = line.split(" ")
                # sum file sizes
                if len(values) > 1:
                    size += int(values[-2])

        return size, None, None, None
        

    def checkExists(self, source, proxy = None, opt = "", tout = None):
        """
        file exists?
        """
        try:
            size, owner, group, permMode = self.listFile(source, proxy, opt)
            if size >= 0:
                return True
        except NotExistsException:
            return False
        except OperationException:
            return False
        return False

    def checkDirExists(self, fullSource, opt = "", tout = None):
        """
        Dir exists?
        """

        cmd = "srmls -recursion_depth=0 " + opt +" "+ fullSource

        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            return False
        return True

    def getTurl(self, source, proxy = None, opt = "", tout = None):
        """
        return the gsiftp turl
        """
        fullSource = source.getLynk()
        cmd = ""
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        opt += " -T srmv2 -D srmv2 "
        cmd += "lcg-gt " + opt + " " + fullSource + " gsiftp"
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0]

    def listPath(self, source, proxy = None, opt = "", tout = None):
        """
        srmls

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmls " + opt +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            if str(problems).find("such file or directory") != -1 or \
               str(problems).find("does not exist") != -1 or \
               str(problems).find("failed in some way or another") != -1 or \
               str(problems).find("srm_failure") != -1 or \
               (str(problems).find("not found") != -1 and \
               str(problems).find("cacheexception") != -1):
                return False
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )

        ### need to parse the output of the commands ###
        filesres = []
        # for each listed file
        for line in outputs.split("\n"):
            # get correct lines
            if line.find(source.getFullPath()) != -1 and line.find("surl[0]=") == -1:
                values = line.split(" ")
                # sum file sizes
                if len(values) > 1:
                    filesres.append(values[-1])

        return filesres


# srm-reserve-space srm-release-space srmrmdir srmmkdir srmping srm-bring-online
