"""
Class interfacing with gsiftp end point
(not using only lcg-utils)
"""

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
            if line.find("No entries for host") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("No such file or directory") != -1 or \
               line.find("error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("Unknown option") != -1 or \
                 line.find("unrecognized option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)

        return problems

    def createDir(self, source, proxy = None, opt = ""):
        """
        edg-gridftp-mkdir
        """
        fullSource = source.getLynk()
        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-mkdir " + opt + " "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +source.workon+ "].", problems, outputs)

    def copyLcg(self, source, dest, proxy = None, opt = ""):
        """
        lcg-cp
        """
        fullSource = source.getLynk()
        fullDest = dest.getLynk()

        setProxy = ''
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + ";"

        cmd = setProxy + " lcg-cp " + opt + " --vo cms " + \
                           fullSource + " " + fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )


    def copy(self, source, dest, proxy = None, opt = ""):
        """
        lcg-cp
        """
        from os.path import join
        from types import StringType, UnicodeType
        import tempfile
        import os

        sourcesList = []
        destsList   = []

        if type(source.workon) == StringType or \
           type(source.workon) == UnicodeType:
           # self.copyLcg(source, dest, proxy, opt)
            sourcesList.append(source.getLynk())
        else:
            for onesource in source.workon:
                fullSource = ""
                if source.full:
                    if source.protocol != "local":
                        fullSource = join(source.hostname, onesource)
                    else:
                        if source.hostname != "/":
                            fullSource = "file://" + join(source.hostname, onesource)
                        else:
                            raise TransferException("Error for the path '/'")
                elif source.protocol != "local":
                    fullSource = "gsiftp://" + source.hostname + join("/", onesource)
                else:
                    if source.hostname != "/":
                        fullSource = "file://" + join("/", onesource)
                    else:
                        raise TransferException("Error for the path '/'")
                sourcesList.append(fullSource)
        if type(dest.workon) == StringType or \
           type(dest.workon) == UnicodeType:
            destsList = [dest.getLynk()]*len(sourcesList)
        else:
           for destination in dest.workon:
               fullDest = ""
               if dest.full:
                   if dest.protocol != "local":
                        fullDest = join(dest.hostname, destination)
                   else:
                        if dest.hostname != "/":
                            fullDest = "file://" + join(dest.hostname, destination)
                        else:
                            raise TransferException("Error for the path '/'")
               elif dest.protocol != "local":
                   fullDest = "gsiftp://" + dest.hostname + join("/", destination)
               else:
                   if dest.hostname != "/":
                        fullDest = "file://" + join("/", destination)
                   else:
                        raise TransferException("Error for the path '/'")
               destsList.append( fullDest )

        setProxy = ''  
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + ";"

        fname = None
        exitcode, outputs = "", ""
        try:
            tmp, fname = tempfile.mkstemp( "", "seapi_", os.getcwd() )
            os.close( tmp )
            toCopy = "\n".join([t[0] + " " + t[1] for t in map(None, sourcesList, destsList)]) + "\n"
            super(ProtocolGsiFtp, self).__logout__("To copy: \n%s"%toCopy)
            file(fname, 'w').write( toCopy )
            cmd = setProxy + " globus-url-copy -f " + fname
            exitcode, outputs = self.executeCommand(cmd)
        finally:
            os.unlink( fname )
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +str(source.workon)+ "] to [" \
                                    + str(dest.workon) + "]", problems, outputs )


    def move(self, source, dest, proxy = None, opt = ""):
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

    def deleteRec(self, source, proxy = None, opt = ""):
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

    def listFile(self, source, proxy = None, opt = ""):
        """
        list of dir [edg-gridftp-ls]
        """
        fullSource = source.getLynk()
        opt += " --verbose "
        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + opt + " " + fullSource
        exitcode, outputs = self.executeCommand(cmd)

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

    def delete(self, source, proxy = None, opt = ""):
        """
        edg-gridftp-rm/dir
        """
        fullSource = source.getLynk()

        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-rm " + opt + " " + fullSource
        #if self.checkNotDir(source, proxy, opt = ""):
        #    cmd = "edg-gridftp-rm " + options + " " + fullSource
        #else:
        #    cmd = "edg-gridftp-rmdir " + options + " " + fullSource
            
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy = None, opt = ""):
        """
        edg-gridftp-ls
        """
        fullSource = source.getLynk()
        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + opt + " " + fullSource
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


    def checkPermission(self, source, proxy = None, opt = ""):
        """
        edg-gridftp-ls
        """
        fullSource = source.getLynk()
        opt += " --verbose "
        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + opt + " " + fullSource + " | awk '{print $1}'"
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            return self.__convertPermission__(outputs)
        return outputs

    def getFileSize(self, source, proxy = None, opt = ""):
        """
        edg-gridftp-ls
        """
        fullSource = source.getLynk()
        opt += " --verbose "
        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + opt + " " + fullSource + " | awk '{print $5}'"
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            return outputs
        raise OperationException("Error getting size for [" +source.workon+ "]",
                                  problems, outputs )
 
    def getTurl(self, source, proxy = None, opt = ""):
        """
        return the gsiftp turl
        """
        return source.getLynk()

    def listPath(self, source, proxy = None, opt = ""):
        """
        list of dir [edg-gridftp-ls]
        """
        fullSource = source.getLynk()
        if proxy is not None:
            opt += " --proxy=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = "edg-gridftp-ls " + opt + " " + fullSource
        exitcode, outputs = self.executeCommand(cmd)
        
        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ "]", \
                                      outputs, outputs )

        filesres = []
        for filet in outputs.split('\n'):
            import os
            filesres.append( os.path.join(source.getFullPath(), filet) )

        return filesres


