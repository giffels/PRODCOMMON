from Protocol import Protocol
from Exceptions import *

class ProtocolGsiFtp(Protocol):
    """
    implementing the GsiFtp protocol
    """

    def __init__(self):
        super(ProtocolGsiFtp, self).__init__()

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text lookng for Exceptions
        """
        problems = []
        lines = outLines.split("\n")
        for line in lines:
            if line.find("No such file or directory") != -1 or line.find("error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
        return problems

    def createDir(self, source, proxy):
        """
        edg-gridftp-mkdir
        """
        fullSource = source.getLynk()
        options = ""
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-mkdir " + options + " "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +source.workon+ "].", problems, outputs)

    def copy(self, source, dest, proxy):
        """
        lcg-cp
        """
        fullSource = source.getLynk()
        fullDest = dest.getLynk()

        setProxy = ''  
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + ";"
 
        cmd = setProxy + " lcg-cp --vo cms "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy):
        """
        copy() + delete()
        """
        self.copy(source, dest, proxy)
        self.delete(source, proxy)

    def delete(self, source, proxy):
        """
        edg-gridftp-rm
        """
        fullSource = source.getLynk()

        options = ""
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-rm " + options + " " + fullSource

        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy):
        """
        edg-gridftp-ls
        """
        fullSource = source.getLynk()
        options = ""
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + options + " " + fullSource
        exitcode, outputs = self.executeCommand(cmd)
 
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
 
        if exitcode != 0 or len(problems) > 0:
            return False
        return True

    def __convertPermission__(self, drwx):
        owner  = drwx[1:3]
        ownSum = 0
        group  = drwx[4:6]
        groSum = 0
        others = drwx[7:9]
        othSum = 0

        for val in owner:
            if val == "r":   ownSum += 4
            elif val == "w": ownSum += 2
            elif val == "x": ownSum += 1
        for val in group:
            if val == "r":   groSum += 4
            elif val == "w": groSum += 2
            elif val == "x": groSum += 1
        for val in others:
            if val == "r":   othSum += 4
            elif val == "w": othSum += 2
            elif val == "x": othSum += 1

        return [ownSum, groSum, othSum]


    def checkPermission(self, source, proxy = None):
        """
        edg-gridftp-ls
        """
        fullSource = source.getLynk()
        options = " --verbose "
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + options + " " + fullSource + " | awk '{print $1}'"
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            return self.__convertPermission__(outputs)
        return outputs

    def getFileSize(self, source, proxy = None):
        """
        edg-gridftp-ls
        """
        fullSource = source.getLynk()
        options = " --verbose "
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + options + " " + fullSource + " | awk '{print $5}'"
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            return outputs
        raise OperationException("Error getting size for [" +source.workon+ "]",
                                  problems, outputs )
 
    def getTurl(self, source, proxy = None):
        """
        return the gsiftp turl
        """
        return source.getLynk()
