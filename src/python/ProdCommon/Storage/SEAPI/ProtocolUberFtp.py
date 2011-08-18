"""
Class interfacing with UberFtp end point
(not using only lcg-utils)
"""

from Protocol import Protocol
from Exceptions import *
import os

class ProtocolUberFtp(Protocol):
    """
    implementing the GsiFtp protocol
    """

    def __init__(self):
        super(ProtocolUberFtp, self).__init__()

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
            if line.find("no entries for host") != -1 or \
               line.find("no route to host") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("no such file or directory") != -1 or \
               line.find("error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("unknown option") != -1 or \
                 line.find("unrecognized option") != -1 or \
                 line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)
            elif line.find("command not found") != -1 or \
                 line.find("command not found") != -1 or \
                 line.find("unknown command") != -1:
                raise MissingCommand("Command not found: client not " \
                                     "installed or wrong environment", \
                                     [line], outLines)
        return problems

    def createDir(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> mkdir
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"mkdir %s\" " % (source.hostname, os.path.join("/", source.workon))
 
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###
 
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +source.workon+ "].", problems, outputs)

    def copy(self, source, dest, proxy = None, opt = "", tout = None):
        """
        Uberftp --> lcg-cp
        """
        
        precmd = ''  
        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)
 
        #fix uberftp local file references
        srcPath, destPath =  (source.getLynk(), dest.getLynk() )
        srcPath = srcPath.replace('file://', 'file:')
        destPath = destPath.replace('file://', 'file:')
        
        cmd = precmd + " uberftp %s %s " % ( srcPath, os.path.dirname(destPath) )
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

    def getWalkList(self, source, proxy = None):
        """
        _getWalkList_

        return the list of file inside a given path
        """ 
        filelist = []
        dirlist = []
        for k, v in self.listFile(source, proxy).items():
            if v == "d":
                source.workon = k
                dirlist.append(k)
                filelistT, dirlistT = self.getWalkList(source, proxy)
                dirlist += dirlistT
                filelist += filelistT
            elif v == "f":
                filelist.append(k)
        return filelist, dirlist

    def deleteRec(self, source, proxy = None, opt = "", tout = None):
        """
        _deleteRec_
        """
        filelist, dirlist = self.getWalkList(source, proxy)
        for filet in filelist:
            source.workon = filet
            self.delete(source, proxy, opt)
            source.workon = ""
        dirlist.reverse()
        for dirt in dirlist:
            source.workon = dirt
            self.delete(source, proxy, opt)
            source.workon = ""

    def listFile(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> ls
        """

        precmd = ""

        opt += " --verbose "
        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" " % (source.hostname, os.path.join("/", source.workon))
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        if exitcode != 0: 
            raise OperationException("Error listing [" +source.workon+ "]", \
                                      outputs, outputs )

        dirname = {}
        for filet in outputs.split('\n'):
            if len(filet) > 0 and filet != None:
                basename = filet.split(" ")[-1].replace("\r", "")
                if basename != "." and basename != "..":
                    import os
                    dirpath = os.path.join(source.workon, basename)
                    if filet[0] == "-":
                        dirname.setdefault(dirpath, "f")
                    elif filet[0] == "d":
                        dirname.setdefault(dirpath, "d")
        return dirname

    def checkNotDir(self, source, proxy = None):
        """
        _checkNotDir_
        """
        dirall = self.listFile(source, proxy)
        flag = False
        for k, v in dirall.items(): 
            if v == "f":
                flag = True
            else:
                flag = False
                break
        return flag

    def delete(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> rm
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"rm %s\" " % (source.hostname, os.path.join("/", source.workon))

        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if ' Is a directory' in problems:
            self.deleteDir( source, proxy, opt )
        elif exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )
        
    def deleteDir(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> rmdir
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"rmdir %s\" " % (source.hostname, os.path.join("/", source.workon))

        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###

        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> ls
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" " % \
              (source.hostname, os.path.join("/", source.workon))
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
 
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


    def checkPermission(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> ls
        """

        precmd = ""

        opt += " --verbose "
        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" | awk '{print $1}' " % (source.hostname, os.path.join("/", source.workon))
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            return self.__convertPermission__(outputs)
        return outputs

    def getFileSize(self, source, proxy = None, opt = "", tout = None):
        """
        Uberftp -> ls
        """

        precmd = ""

        opt += " --verbose "
        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" | awk '{print $5}' " % (source.hostname, os.path.join("/", source.workon))
        
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error getting size for [" +source.workon+ "]",
                                      problems, outputs )
        return outputs
 
    def getTurl(self, source, proxy = None, opt = "", tout = None):
        """
        return the gsiftp turl
        """
        return source.getLynk()

    def listPath(self, source, proxy = None, opt = "", tout = None):
        """
        list of dir [uberftp -> ls]
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" " % (source.hostname, os.path.join("/", source.workon))
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        
        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ "]", \
                                      outputs, outputs )

        filesres = []
        for filet in outputs.split('\n'):
            if filet != "." and filet != "..":
                filesres.append( os.path.join(source.getFullPath(), filet) )

        return filesres


