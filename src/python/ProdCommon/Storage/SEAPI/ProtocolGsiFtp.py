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

    def getWalkList(self, source, proxy):
        filelist = []
        dirlist = []
        for k, v in self.listPath(source, proxy).items():
            if v == "d":
                source.workon = k
                dirlist.append(k)
                filelistT, dirlistT = self.getWalkList(source, proxy)
                dirlist += dirlistT
                filelist += filelistT
            elif v == "f":
                filelist.append(k)
        return filelist, dirlist

    def deleteRec(self, source, proxy):
        filelist, dirlist = self.getWalkList(source, proxy)
        for file in filelist:
            source.workon = file 
            self.delete(source, proxy)
            source.workon = ""
        dirlist.reverse()
        for dir in dirlist:
            source.workon = dir
            self.delete(source, proxy)
            source.workon = ""

    def listPath(self, source, proxy):
        """
        list of dir [edg-gridftp-ls]
        """
        fullSource = source.getLynk()
        options = " --verbose "
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + options + " " + fullSource
        exitcode, outputs = self.executeCommand(cmd)

        if exitcode != 0: 
            raise OperationException("Error listing [" +source.workon+ "]", \
                                      outputs, outputs )

        dirname = {}
        for file in outputs.split('\n'):
            if len(file) > 0 and file != None:
                basename = file.split(" ")[-1].replace("\r", "")
                if basename != "." and basename != "..":
                    import os
                    if file[0] == "-":
                        dirname.setdefault(os.path.join(source.workon, basename), "f")
                    elif file[0] == "d":
                        dirname.setdefault(os.path.join(source.workon, basename), "d")
        return dirname

    def checkNotDir(self, source, proxy):
        dirall = self.listPath(source, proxy)
        flag = False
        for k, v in dirall.items(): 
            if v == "f":
                flag = True
            else:
                flag = False
                break;
        return flag

    def delete(self, source, proxy):
        """
        edg-gridftp-rm/dir
        """
        fullSource = source.getLynk()

        options = ""
        if proxy is not None:
            options = "--proxy=" + str(proxy)
            self.checkUserProxy(proxy)

        cmd = ""
        if self.checkNotDir(source, proxy):
            cmd = "edg-gridftp-rm " + options + " " + fullSource
        else:
            cmd = "edg-gridftp-rmdir " + options + " " + fullSource
            
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
