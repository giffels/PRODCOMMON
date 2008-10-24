"""
Class interfacing with srm version 1 end point
"""

from Protocol import Protocol
from Exceptions import *

class ProtocolSrmv1(Protocol):
    """
    implementing the srm protocol version 1.*
    """

    def __init__(self):
        super(ProtocolSrmv1, self).__init__()
        self._options = " -debug=true " 

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text lookng for Exceptions
        """
        problems = []
        lines = outLines.split("\n")
        for line in lines:
            if line.find("Exception") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("unrecognized option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)

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

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)
        
        cmd = "srmcp " +opt +" "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0: #or len(problems) > 0: #if exit code = 0 => skip
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None, opt = ""):
        """
        srmcp + delete()
        """
        if self.checkExists(dest, proxy, opt):
            problems = ["destination file already existing", dest.workon]
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems)
        self.copy(source, dest, proxy, opt)
        if self.checkExists(dest, proxy, opt):
            self.delete(source, proxy, opt)
        else:
            raise TransferException("Error deleting [" +source.workon+ "]", \
                                     ["Uknown Problem"] )

    def deleteRec(self, source, proxy = None, opt = ""):
        """
        _deleteRec_
        """
        self.delete(source, proxy, opt)

    def delete(self, source, proxy = None, opt = ""):
        """
        srm-advisory-delete
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srm-advisory-delete " +opt +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkPermission(self, source, proxy = None, opt = ""):
        """
        return file/dir permission
        """
        return int(self.listPath(source, proxy, opt)[3])

    def getFileSize(self, source, proxy = None, opt = ""):
        """
        file size
        """
        ##size, owner, group, permMode = self.listPath(filePath, SEhost, port)
        size = self.listPath(source, proxy, opt)[0]
        return int(size)

    def listPath(self, source, proxy = None, opt = ""):
        """
        srm-get-metadata

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        opt += self._options
        if proxy is not None:
            opt += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srm-get-metadata " +opt +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )

        ### need to parse the output of the commands ###
        size, owner, group, permMode = "", "", "", ""
        for line in outputs.split("\n"):
            if line.find("    size ") != -1:
                size = line.split(":")[1].strip()
            elif line.find("    owner ") != -1:
                owner = line.split(":")[1].strip()
            elif line.find("    group ") != -1:
                group = line.split(":")[1].strip()
            elif line.find("    permMode ") != -1:
                permMode = line.split(":")[1].strip()
        if size == "" or owner == "" or group == "" or permMode == "":
            raise NotExistsException("Path [" + source.workon + \
                                     "] does not exists.")
        return int(size), owner, group, permMode
        

    def checkExists(self, source, proxy = None, opt = ""):
        """
        file exists?
        """
        try:
            size, owner, group, permMode = self.listPath(source, proxy, opt)
            if size is not "" and owner is not "" and\
               group is not "" and permMode is not "":
                return True
        except NotExistsException:
            return False
        except OperationException:
            return False
        return False

    def getTurl(self, source, proxy = None, opt = ""):
        """
        return the gsiftp turl
        """
        fullSource = source.getLynk()
        cmd = ""
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        opt += " -T srmv1 -D srmv1 "

        cmd += "lcg-gt " + opt + " " + str(fullSource) + " gsiftp"
        exitcode, outputs = self.executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0]

# srm-reserve-space srm-release-space srmrmdir srmmkdir srmping srm-bring-online
