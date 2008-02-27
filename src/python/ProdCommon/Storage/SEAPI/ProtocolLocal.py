from Protocol import Protocol
from Exceptions import * 
import os

class ProtocolLocal(Protocol):
 
    def __init__(self, SEpath, name):
        if self.checkExists(SEpath):
            Protocol.__init__(self, SEpath, None, name)
            self._path = SEpath
        else:
            raise NotExistsException("Path [" + SEpath + "] does not exists")
 
    def move(self, source, dest):
        exit_code = -1
        outputStr = ""
        if self.checkExists(source):
            cmd = "mv "+ source +" "+ dest
            exit_code, outputStr = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source + \
                                     "] does not exists.")
        if exit_code != 0:
            raise TransferException("Error moving [" +source+ "] to [" +dest+ \
                                    "]\n "+outputStr)
 
        return outputStr
 
    def copy(self, source, dest):
        exit_code = -1
        outputStr = ""
        if self.checkExists(source):
            cmd = "cp " + source + " " + dest
            exit_code, outputStr = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + source + \
                                     "] does not exists.")
        if exit_code != 0:
            raise TransferException("Error coping [" +source+ "] to [" +dest+ \
                                    "]\n "+outputStr)
         
        return outputStr
 
    def delete(self, filePath):
        exit_code = -1
        outputStr = ""
        if self.checkExists(filePath):
            cmd = "rm -rf " + filePath
            exit_code, outputStr = self.executeCommand(cmd)
        else:
            raise NotExistsException("Error: path [" + filePath + \
                                     "] does not exists.")
        if exit_code != 0:
            raise ProtocolOperationException("Error deleting [" +filePath+ \
                                             "]\n "+outputStr)
 
        return outputStr
 
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
 
    def checkPermission(self, filePath):
        exit_code = -1
        outputStr = ""
        if self.checkExists(filePath):
            cmd = "ls -la " + filePath + " | awk '{print $1}'"
            exit_code, outputStr = self.executeCommand(cmd)
            if exit_code == 0:
                outputStr = self.__convertPermission__(outputStr)
            else:
                raise TransferException("Error checking [" +filePath+ \
                                        "]\n "+outputStr)
        else:
            raise ProtocolOperationException("Error: path [" + filePath + \
                                             "] does not exists.")
 
        return outputStr
 
    def getFileSize(self, filePath):
        sizeFile = ""
        if self.checkExists(filePath):
            try:
                from os.path import getsize
                sizeFile = getsize ( filePath )
            except OSError:
                return 0
        else:
            raise NotExistsException("Error: path [" + filePath + \
                                     "] does not exists.")
 
        return int(sizeFile)
 
    def getDirSize(self, fullPath):
        if self.checkExists(fullPath):
            from os.path import join
            summ = 0
            for path, dirs, files in os.walk( fullPath, topdown=False):
                for name in files:
                    summ += self.getFileSize ( join(path, name) )
                for name in dirs:
                    summ += self.getFileSize ( join(path, name) )
            summ += self.getFileSize(fullPath)
            return summ
        else:
            raise ProtocolOperationException("Error: path [" + fullPath + \
                                             "] does not exists.")
        
 
    def listPath(self, fullPath):
        exit_code = -1
        outputStr = ""
        if self.checkExists(fullPath):
            cmd = "ls " + fullPath
            exit_code, outputTemp = self.executeCommand(cmd)
            outputStr = outputTemp.split("\n")
        else:
            raise ProtocolOperationException("Error: path [" + fullPath + \
                                             "] does not exists.")
        if exit_code != 0:
            raise ProtocolOperationException("Error listing [" +fullPath+ \
                                             "]\n "+outputStr)
 
        return outputStr
 
    def checkExists(self, filePath):
        return os.path.exists(filePath)
 
    def getGlobalQuota(self):
        cmd = "df " + self._path + " | awk '{ print $5,$4,$3 }'"
        exit_code, outputTemp = self.executeCommand(cmd)
        if exit_code != 0:
            raise ProtocolOperationException("Error getting local quota for [" \
                                             +self._path+ "]\n " +outputTemp)
        val = outputTemp.split("\n")[1].split(" ")
        return val
