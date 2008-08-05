from Protocol import Protocol
from Exceptions import *

class ProtocolLcgUtils(Protocol):
    """
    implementing storage interaction with lcg-utils 
    """

    def __init__(self):
        super(ProtocolLcgUtils, self).__init__()
        self.options  = " --verbose "
        self.options += " --vo=cms "

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text lookng for Exceptions
        """
        problems = []
        lines = outLines.split("\n")
        for line in lines:
            if line.find("No such file or directory") != -1 or line.find("error") != -1 or line.find("Failed") != -1 :
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
        return problems

    def createDir(self, source, proxy):
        """
        edg-gridftp-mkdir
        """
        pass

    def copy(self, source, dest, proxy):
        """
        lcg-cp
        """
        fullSource = source.getLynk()
        fullDest = dest.getLynk()

        setProxy = ''  
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "
 
        cmd = setProxy + " lcg-cp "+ self.options +" "+ fullSource +" "+ fullDest
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

        setProxy = ''
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "

        cmd = setProxy + "lcg-del "+ self.options +" " + fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy):
        """
        workaround with lcg-gt
        """
        try:
            self.getTurl(source, proxy)
        except:
            return False
        return True

    def getTurl(self, source, proxy = None):
        """
        return the gsiftp turl
        """
        fullSource = source.getLynk()
        cmd = ""
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        cmd += "lcg-gt " + str(fullSource) + " gsiftp"
        exitcode, outputs = self.executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)
        
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0] 

