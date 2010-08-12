"""
Class interfacing with local unix file system
(posix like)
"""

from Protocol import Protocol
from Exceptions import * 
import os

class ProtocolLocal(Protocol):
    """
    implementing a "local protocol", using unix system commmands
    """
 
    def __init__(self):
        super(ProtocolLocal, self).__init__()

    def move(self, source, dest, opt = "", tout = None):
        """
        move from source.workon to dest.workon
        """
        exitcode = -1
        outputs = ""
        ll = dest.getLynk()
        dest_fullpath = ll.split("file://",1)[1] 
        
        if self.checkExists(source, opt):
            cmd = "mv " + opt + " "+ source.workon +" "+ dest_fullpath
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    +dest_fullpath+ "]\n " +outputs)
 
    def copy(self, source, dest, opt = "", tout = None):
        """
        copy from source.workon to dest.workon
        """
        exitcode = -1
        outputs = ""
        ll = dest.getLynk()
        dest_fullpath = ll.split("file://",1)[1]

        if self.checkExists(source, opt):
            cmd = "cp " + opt + " " + source.workon + " " + dest_fullpath
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    +dest_fullpath+ "]\n " +outputs)
 
    def delete(self, source, opt = "", tout = None):
        """
        _delete_
        """
        exitcode = -1
        outputs = ""
        ll = source.getLynk()
        source_fullpath = ll.split("file://",1)[1]
        if self.checkExists(source, opt):
            cmd = "rm " + opt + " " + source_fullpath
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        else:
            raise NotExistsException("Error: path [" + source_fullpath + \
                                     "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error deleting [" +source_fullpath \
                                             + "]\n "+outputs)

    def createDir(self, source, opt = "", tout = None):
        """
        _createDir_
        """
        outputs = ""
        ll = source.getLynk()
        source_fullpath = ll.split("file://",1)[1]
        if self.checkExists(source, opt)  is False:
            cmd = "/bin/mkdir -m 775 -p " + opt + " " + source_fullpath
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode != 0:
                raise OperationException("Error creating [" +source_fullpath \
                                             + "]\n "+outputs)
        else:
            print "directory" + source_fullpath + "already exists"
 
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
 
    def checkPermission(self, source, opt = "", tout = None):
        """
        _checkPermission_
        """
        exitcode = -1
        outputs = ""
        ll = source.getLynk()
        source_fullpath = ll.split("file://",1)[1]
        if self.checkExists(source, opt):
            cmd = "ls -la " + opt + " " + source_fullpath + " | awk '{print $1}'"
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode == 0:
                outputs = self.__convertPermission__(outputs)
            else:
                raise TransferException("Error checking [" +source_fullpath+ \
                                        "]\n "+outputs)
        else:
            raise OperationException("Error: path [" + source_fullpath + \
                                             "] does not exists.")
 
        return outputs
 
    def getFileSize(self, source, credential = None, opt = "", tout = None):
        """
        _getFileSize_
        """
        sizeFile = 0
        if self.checkExists(source, opt):
            try:
                from os.path import getsize
                sizeFile = int(getsize ( source.workon ))
            except OSError:
                return -1
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
 
        return int(sizeFile)
 
    def getDirSize(self, source, opt = "", tout = None):
        """
        _getDirSize_
        """
        if self.checkExists(source, opt):
            from os.path import join, getsize
            summ = 0
            for path, dirs, files in os.walk( source.workon, topdown=False):
                for name in files:
                    summ += getsize ( join(path, name) )
                for name in dirs:
                    summ += getsize ( join(path, name) )
            summ += getsize(source.workon)
            return summ
        else:
            raise OperationException("Error: path [" + source.workon + \
                                             "] does not exists.")
        
    def listPath(self, source, opt = "", tout = None):
        """
        _listPath_
        """
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "ls " + opt + " " + source.workon
            exitcode, outputTemp = self.executeCommand(cmd, timeout = tout)
            outputs = outputTemp.split("\n")
        else:
            raise OperationException("Error: path [" + source.workon + \
                                             "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ \
                                             "]\n "+outputs)
 
        return outputs
 
    def checkExists(self, source, opt = "", tout = None):
        """
        _checkExists_
        """
        ll = source.getLynk()
        fullpath = ll.split("file://",1)[1] 
        out = os.path.exists( fullpath )
        return out
 
    def getGlobalQuota(self, source, opt = "", tout = None):
        """
        _getGlobalQuota_
        """
        cmd = "df " + opt + " " + source.workon + " | awk '{ print $5,$4,$3 }'"
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        if exitcode != 0:
            raise OperationException("Error getting local quota for [" \
                                             +source.workon+ "]\n " +outputs)
        val = outputs.split("\n")[1].split(" ")
        return val
