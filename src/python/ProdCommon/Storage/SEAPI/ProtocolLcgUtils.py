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
            if line.find("No such file or directory") != -1 or \
               line.find("error") != -1 or line.find("Failed") != -1 or \
               line.find("CacheException") != -1 or \
               line.find("No entries for host") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("Unknown option") != -1 or \
                 line.find("unrecognized option") != -1 or \
                 line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", [], outLines)
 
        return problems

    def createDir(self, source, proxy = None, opt = ""):
        """
        edg-gridftp-mkdir
        """
        pass

    def copy(self, source, dest, proxy = None, opt = ""):
        """
        lcg-cp
        """
        fullSource = source.getLynk()
        fullDest = dest.getLynk()

        setProxy = ''  
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "
 
        cmd = setProxy + " lcg-cp "+ self.options +" " + opt + " "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None, opt = ""):
        """
        copy() + delete()
        """
        self.copy(source, dest, proxy, opt)
        self.delete(source, proxy, opt)

    def delete(self, source, proxy = None, opt = ""):
        """
        lcg-del
        """
        fullSource = source.getLynk()

        setProxy = ''
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "

        cmd = setProxy + "lcg-del "+ self.options +" " + opt + " " + fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy = None, opt = ""):
        """
        lcg-ls (lcg-gt)
        """
        if source.protocol in ["gsiftp-lcg"]:
            try:
                self.getTurl(source, proxy, opt)
            except:
                return False
            return True
        else:
            fullSource = source.getLynk()
            cmd = ""
            if proxy is not None:
                cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
                self.checkUserProxy(proxy)
            cmd += "lcg-ls " + opt + " " + fullSource
            exitcode, outputs = self.executeCommand(cmd)
            problems = self.simpleOutputCheck(outputs)
            if exitcode != 0 or len(problems) > 0:
                if str(problems).find("No such file or directory") != -1 or \
                   (str(problems).find("not found") != -1 and \
                    str(problems).find("CacheException") != -1):
                    return False
                raise OperationException("Error checking [" +source.workon+ "]", \
                                         problems, outputs )
            return True

    def getFileSize(self, source, proxy = None, opt = ""):
        """
        lcg-ls
        """
        fullSource = source.getLynk()
        cmd = ""
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        cmd = "lcg-ls -l " + opt + " "+ fullSource +" | awk '{print $5}'"
        exitcode, outputs = self.executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return int(outputs)


    def getTurl(self, source, proxy = None, opt = ""):
        """
        return the gsiftp turl
        """
        fullSource = source.getLynk()
        cmd = ""
        if proxy is not None:
            cmd += 'export X509_USER_PROXY=' + str(proxy) + ' && '
            self.checkUserProxy(proxy)
        cmd += "lcg-gt " + opt + " " + str(fullSource) + " gsiftp"
        exitcode, outputs = self.executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)
        
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        return outputs.split('\n')[0] 

