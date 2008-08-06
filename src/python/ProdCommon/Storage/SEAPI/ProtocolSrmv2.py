from Protocol import Protocol
from Exceptions import *

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
            if line.find("Exception") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
        return problems

    def copy(self, source, dest, proxy = None):
        """
        srmcp
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
        
        cmd = "srmcp " +option +" "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None):
        """
        with srmmv "source and destination have to have same URL type"
         => copy and delete
        """
        if self.checkExists(dest, proxy):
            problems = ["destination file already existing", dest.workon]
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems)
        self.copy(source, dest, proxy)
        if self.checkExists(dest, proxy):
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

    def deleteRec(self, source, proxy):
        self.delete(source, proxy)

    def delete(self, source, proxy = None):
        """
        srmrm
        """
        fullSource = source.getLynk()

        option = self._options + " -retry_num=1 "
        if proxy is not None:
            option += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmrm " +option +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def createDir(self, source, proxy = None):
        """
        srmmkdir
        """
        fullSource = source.getLynk()

        option = self._options + " -retry_num=1 "
        if proxy is not None:
            option += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmmkdir " +option +" "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error creating [" +source.workon+ "]", \
                                      problems, outputs )

    def checkPermission(self, source, proxy = None):
        """
        return file/dir permission
        """
        return int(self.listPath(source, proxy)[3])

    def getFileSize(self, source, proxy = None):
        """
        file size
        """
        ##size, owner, group, permMode = self.listPath(filePath, SEhost, port)
        size = self.listPath(source, proxy)[0]
        return int(size)

    def listPath(self, source, proxy = None):
        """
        srmls

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        option = self._options + " -retry_num=0 "
        if proxy is not None:
            option += " -x509_user_proxy=%s " % proxy
            self.checkUserProxy(proxy)

        cmd = "srmls " +option +" "+ fullSource
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
        

    def checkExists(self, source, proxy = None):
        """
        file exists?
        """
        try:
            size, owner, group, permMode = self.listPath(source, proxy)
            if size is not "" and owner is not "" and\
               group is not "" and permMode is not "":
                return True
        except NotExistsException:
            return False
        except OperationException:
            return False


    def getTurl(self, source, proxy = None):
        """
        return the gsiftp turl
        """
        fullSource = source.getLynk()
        cmd = ""
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        options = " -T srmv2 -D srmv2 "
        cmd += "lcg-gt " + str(options) + " " + str(fullSource) + " gsiftp"
        exitcode, outputs = self.executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0]

# srm-reserve-space srm-release-space srmrmdir srmmkdir srmping srm-bring-online
