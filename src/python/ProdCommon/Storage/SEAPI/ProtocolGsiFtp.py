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
        """
        fullSource = source.getLynk()
        cmd = "export X509_USER_PROXY=" + proxy + "; edg-gridftp-mkdir "+ fullSource
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

        cmd = "export X509_USER_PROXY=" + proxy + "; lcg-cp --vo cms "+ fullSource +" "+ fullDest
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
        lcg-del
        """
        fullSource = source.getLynk()

        cmd = "export X509_USER_PROXY=" + proxy + "; lcg-del "+ fullSource
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
        
        cmd = "export X509_USER_PROXY=" + proxy + "; edg-gridftp-ls "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)
 
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
 
        if exitcode != 0 or len(problems) > 0:
            return False
        return True
 
