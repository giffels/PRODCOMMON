from Protocol import Protocol
from Exceptions import * 
import os

class ProtocolLocal(Protocol):
    """
    implementing a "local protocol", using unix system commmands
    """
 
    def __init__(self):
        super(ProtocolLocal, self).__init__()

    def move(self, source, dest, opt = ""):
        """
        move from source.workon to dest.workon
        """
        exitcode = -1
        outputs = ""
        
        if self.checkExists(source, opt):
            cmd = "mv " + opt + " "+ source.workon +" "+ dest.workon
            exitcode, outputs = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    +dest.workon+ "]\n " +outputs)
 
    def copy(self, source, dest, opt = ""):
        """
        copy from source.workon to dest.workon
        """
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "cp " + opt + " " + source.workon + " " + dest.workon
            exitcode, outputs = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    +dest.workon+ "]\n " +outputs)
 
    def delete(self, source, opt = ""):
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "rm -rf " + opt + " " + source.workon
            exitcode, outputs = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error deleting [" +source.workon \
                                             + "]\n "+outputs)

    def createDir(self, source, opt = ""):
        exitcode = -1
        outputs = ""
        cmd = "mkdir " + opt + " " + source.workon
        exitcode, outputs = self.executeCommand(cmd)
        if exitcode != 0:
            raise OperationException("Error creating [" +source.workon \
                                             + "]\n "+outputs)
 
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
 
    def checkPermission(self, source, opt = ""):
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "ls -la " + opt + " " + source.workon + " | awk '{print $1}'"
            exitcode, outputs = self.executeCommand(cmd)
            if exitcode == 0:
                outputs = self.__convertPermission__(outputs)
            else:
                raise TransferException("Error checking [" +source.workon+ \
                                        "]\n "+outputs)
        else:
            raise OperationException("Error: path [" + source.workon + \
                                             "] does not exists.")
 
        return outputs
 
    def getFileSize(self, source, opt = ""):
        sizeFile = ""
        if self.checkExists(source, opt):
            try:
                from os.path import getsize
                sizeFile = getsize ( source.workon )
            except OSError:
                return -1
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
 
        return int(sizeFile)
 
    def getDirSize(self, source, opt = ""):
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
        
    def listPath(self, source, opt = ""):
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "ls " + opt + " " + source.workon
            exitcode, outputTemp = self.executeCommand(cmd)
            outputs = outputTemp.split("\n")
        else:
            raise OperationException("Error: path [" + source.workon + \
                                             "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ \
                                             "]\n "+outputs)
 
        return outputs
 
    def checkExists(self, source, opt = ""):
        return os.path.exists(source.workon)
 
    def getGlobalQuota(self, source, opt = ""):
        cmd = "df " + opt + " " + source.workon + " | awk '{ print $5,$4,$3 }'"
        exitcode, outputs = self.executeCommand(cmd)
        if exitcode != 0:
            raise OperationException("Error getting local quota for [" \
                                             +source.workon+ "]\n " +outputs)
        val = outputs.split("\n")[1].split(" ")
        return val
