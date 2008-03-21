from Protocol import Protocol
from Exceptions import * 
import os

class ProtocolLocal(Protocol):
    """
    implementing a "local protocol", using unix system commmands
    """
 
    def __init__(self):
        super(ProtocolLocal, self).__init__()

    def move(self, source, dest, proxy = None):
        """
        move from source.workon to dest.workon
        """
        exitcode = -1
        outputs = ""
        
        if self.checkExists(source.workon, proxy):
            cmd = 'export X509_USER_PROXY=' + str(proxy) + ' && '
            cmd += "mv "+ source.workon +" "+ dest.workon
            exitcode, outputs = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    +dest.workon+ "]\n " +outputs)
 
    def copy(self, source, dest, proxy = None):
        """
        copy from source.workon to dest.workon
        """
        exitcode = -1
        outputs = ""
        if self.checkExists(source, proxy):
            cmd = 'export X509_USER_PROXY=' + str(proxy) + ' && '
            cmd += "cp " + source.workon + " " + dest.workon
            exitcode, outputs = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    +dest.workon+ "]\n " +outputs)
 
    def delete(self, source, proxy = None):
        exitcode = -1
        outputs = ""
        if self.checkExists(source, proxy):
            cmd = 'export X509_USER_PROXY=' + str(proxy) + ' && '
            cmd += "rm -rf " + source.workon
            exitcode, outputs = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error deleting [" +source.workon \
                                             + "]\n "+outputs)

    def createDir(self, source, proxy = None):
        exitcode = -1
        outputs = ""
        cmd = 'export X509_USER_PROXY=' + str(proxy) + ' && '
        cmd += "mkdir " + source.workon
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
 
    def checkPermission(self, source, proxy = None):
        exitcode = -1
        outputs = ""
        if self.checkExists(source, proxy):
            cmd = 'export X509_USER_PROXY=' + str(proxy) + ' && '
            cmd += "ls -la " + source.workon + " | awk '{print $1}'"
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
 
    def getFileSize(self, source, proxy = None):
        sizeFile = ""
        if self.checkExists(source, proxy):
            try:
                from os.path import getsize
                sizeFile = getsize ( source.workon )
            except OSError:
                return -1
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
 
        return int(sizeFile)
 
    def getDirSize(self, source, proxy = None):
        if self.checkExists(source, proxy):
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
        
    def listPath(self, source, proxy):
        exitcode = -1
        outputs = ""
        if self.checkExists(source, proxy):
            cmd = 'export X509_USER_PROXY=' + str(proxy) + ' && '
            cmd += "ls " + source.workon
            exitcode, outputTemp = self.executeCommand(cmd)
            outputs = outputTemp.split("\n")
        else:
            raise OperationException("Error: path [" + source.workon + \
                                             "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ \
                                             "]\n "+outputs)
 
        return outputs
 
    def checkExists(self, source, proxy):
        return os.path.exists(source.workon)
 
    def getGlobalQuota(self, source):
        cmd = "df " + source.workon + " | awk '{ print $5,$4,$3 }'"
        exitcode, outputs = self.executeCommand(cmd)
        if exitcode != 0:
            raise OperationException("Error getting local quota for [" \
                                             +source.workon+ "]\n " +outputs)
        val = outputs.split("\n")[1].split(" ")
        return val
