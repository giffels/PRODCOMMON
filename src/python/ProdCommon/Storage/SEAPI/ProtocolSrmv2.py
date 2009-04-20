"""
Class interfacing with srm version 2 end point
"""

from Protocol import Protocol
from Exceptions import *
import string

class ProtocolSrmv2(Protocol):
    """
    implementing the srm protocol version 2.*
    """

    def __init__(self):
        super(ProtocolSrmv2, self).__init__()
        self._options = " -2 -debug=true -protocols=gsiftp,http " 

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text looking for Exceptions
        """
        problems = []
        lines = outLines.split("\n")
        for line in lines:
            if line.find("Exception") != -1 or \
               line.find("does not exist") != -1 or \
               line.find("srm client error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            if line.find("UnknownHostException") != -1 or \
               line.find("No entries for host") != -1: # or \
               #line.find("srm client error") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("SRM_AUTHORIZATION_FAILURE") != -1 or \
               line.find("Permission denied") != -1:
                raise AuthorizationException("Permission denied", [line], outLines)
            elif line.find("Connection timed out") != -1:
                raise OperationException("Connection timed out", [line], outLines)
            elif line.find("already exists") != -1 or \
               line.find("SRM_DUPLICATION_ERROR") != -1:
                raise AlreadyExistsException("File already exists!", \
                                              [line], outLines)
            elif line.find("unrecognized option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)
            elif line.find("Command not found") != -1 or \
                 line.find("command not found") != -1:
                raise MissingCommand("Command not found: client not " \
                                     "installed or wrong environment", \
                                     [line], outLines)
        return problems

    def copy(self, source, dest, proxy = None, opt = ""):
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
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)
        
        cmd = "srmcp " + opt +" "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0: #or len(problems) > 0: #if exit code = 0 => skip
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None, opt = ""):
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

        """
        fullSource = "file:///" + str(source.workon)
        fullDest = "file:///" + str(dest.workon)
        if source.protocol != 'local':
            fullSource = source.getLynk()
        if dest.protocol != 'local':
            fullDest = dest.getLynk()

        option = self._options + " -retry_num=1 "
        if proxy is not None:
            option += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmmv " +option +" "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )
        """

    def deleteRec(self, source, proxy, opt = ""):
        self.delete(source, proxy, opt)

    def delete(self, source, proxy = None, opt = ""):
        """
        srmrm
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmrm " +opt +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def createDir(self, source, proxy = None, opt = ""):
        """
        srmmkdir
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)
        if self.checkDirExists(fullSource , opt = "") is False:
            #tempsource = fullSource
            #if fullSource.find(source.port) != -1:
            #    tempsource = tempsource.split(source.port,1)[-1]
            #elements = tempsource.split('/')
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
                    if self.checkDirExists(fullSource, opt = "" ) is True: break
                else:
                    break
            toCreate.reverse()   
            for i in toCreate:
                fullSource = fullSource+'/'+i
                cmd = "srmmkdir " +opt +" "+ fullSource
                exitcode, outputs = self.executeCommand(cmd)
           
                ### simple output parsing ###
                problems = self.simpleOutputCheck(outputs)
                if exitcode != 0 or len(problems) > 0:
                    raise OperationException("Error creating [" +source.workon+ "]", \
                                              problems, outputs )

    def checkPermission(self, source, proxy = None, opt = ""):
        """
        return file/dir permission
        """
        return int(self.listFile(source, proxy, opt)[3])

    def getFileSize(self, source, proxy = None, opt = ""):
        """
        file size
        """
        ##size, owner, group, permMode = self.listFile(filePath, SEhost, port)
        size = self.listFile(source, proxy, opt)[0]
        return int(size)

    def listFile(self, source, proxy = None, opt = ""):
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
        exitcode, outputs = self.executeCommand(cmd)

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
        

    def checkExists(self, source, proxy = None, opt = ""):
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

    def checkDirExists(self, fullSource, opt = ""):
        """
        Dir exists?
        """

        cmd = "srmls -recursion_depth=0 " + opt +" "+ fullSource

        exitcode, outputs = self.executeCommand(cmd)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            return False
        return True

    def getTurl(self, source, proxy = None, opt = ""):
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
        exitcode, outputs = self.executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0]

    def listPath(self, source, proxy = None, opt = ""):
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
        exitcode, outputs = self.executeCommand(cmd)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
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
