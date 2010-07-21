"""
Implementation of the Protocol interface using a Hadoop storage element
"""

from Protocol import Protocol
from Exceptions import * 
import os

class ProtocolHadoop(Protocol):
    """
    Using the 'hadoop' command set, perform a local stageout.
    """
 
    def __init__(self):
        super(ProtocolHadoop, self).__init__()

    def move(self, source, dest, opt = "", tout = None):
        """
        
        """
        dest_full_path = dest.getLynk()
        exitcode = -1
        outputs = ""
        
        if self.checkExists(source, opt):
            cmd = "hadoop fs -moveFromLocal %s %s" % (source.workon, dest_full_path)
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        else:
            raise NotExistsException("Error: path [" + source.workon+ "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    +dest_full_path+ "]\n " +outputs)
 
    def copy(self, source, dest, proxy, opt = "", tout = None):
        """
        copy from source.workon to dest.workon
        """
        dest_full_path = dest.getLynk()
        exitcode = -1
        outputs = ""

        cmd = "hadoop fs -copyFromLocal %s %s " % (source.workon, dest_full_path)
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        
        if exitcode != 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    +dest_full_path+ "]\n " +outputs)

    def delete(self, source, opt = "", tout = None):
        """
        _delete_
        """
        source_full_path = source.getLynk()
        exitcode = -1
        outputs = ""

        if self.checkExists(source, opt):
            cmd = "hadoop fs -rm %s" %source_full_path
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        else:
            raise NotExistsException("Error: path [" + source_full_path + \
                                     "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error deleting [" +source_full_path \
                                             + "]\n "+outputs)

    def createDir(self, source, opt = "", tout = None):
        """
        _createDir_
        """
        source_full_path = source.getLynk()
        if self.checkExists(source, opt) is False:
            exitcode = -1
            outputs = ""
            cmd = "hadoop fs -mkdir %s" % source_full_path
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode != 0:
                raise OperationException("Error creating [" +source_full_path + "]\n "+outputs)
        #else : print "directory already exists"    
 
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
        source_full_path = source.getLynk()
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "hadoop fs -ls %s | awk '{print $1}' | tail -n 1" %source_full_path
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode == 0:
                outputs = self.__convertPermission__(outputs)
            else:
                raise TransferException("Error checking [" +source_full_path+ \
                                        "]\n "+outputs)
        else:
            raise OperationException("Error: path [" + source_full_path + \
                                             "] does not exists.")
        return outputs
 
    def getFileSize(self, source, opt = "", tout = None):
        """
        _getFileSize_
        """
        source_full_path = source.getLynk()
        sizeFile = 0
        if self.checkExists(source, opt):
            cmd = "hadoop fs -ls %s | awk '{print $5}' | tail -n 1" %source_full_path
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode == 0:
                outputs = int(outputs)
            else:
                raise TransferException("Error checking [" +source_full_path+ \
                                        "]\n "+outputs)
        else:
            raise NotExistsException("Error: path [" + source_full_path + \
                                     "] does not exists.")
        return outputs
 
    def getDirSize(self, source, opt = "", tout = None):
        """
        _getDirSize_
        """
        source_full_path = source.getLynk()

        if self.checkExists(source, opt):
            cmd = "hadoop fs -count %s | awk '{print $3}' | tail -n 1" % \
                source_full_path
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode == 0:
                outputs = int(outputs)
            else:
                raise TransferException("Error checking [" +source_full_path+ \
                                        "]\n "+outputs)
        else:
            raise NotExistsException("Error: path [" + source_full_path + \
                                     "] does not exists.")
        return outputs
        
    def listPath(self, source, opt = "", tout = None):
        """
        _listPath_
        """
        source_full_path = source.getLynk()
        exitcode = -1
        outputs = ""
        if self.checkExists(source, opt):
            cmd = "hadoop fs -ls %s | awk '{print $NF;}'" % source_full_path
            exitcode, outputTemp = self.executeCommand(cmd, timeout = tout)
            outputs = outputTemp.split("\n")
        else:
            raise OperationException("Error: path [" + source_full_path + \
                                             "] does not exists.")
        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ \
                                             "]\n "+outputs)
        return outputs
 
    def checkExists(self, source, opt = "", tout = None):
        """
        _checkExists_
        """
        source_full_path = source.getLynk()
        cmd = "hadoop fs -test -e %s" %source_full_path
        exitcode, _ = self.executeCommand(cmd, timeout = tout)
        if exitcode != 0:
            return False
        return True
 
    def getGlobalQuota(self, source, opt = "", tout = None):
        """
        _getGlobalQuota_
        """
        source_full_path = source.getLynk()
        cmd = "hadoop fs -df %s | tail -n1 | awk '{print $5,$4,$3}'" %source_full_path
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        if exitcode != 0:
            raise OperationException("Error getting local quota for [" \
                                             +source_full_path+ "]\n " +outputs)
        val = outputs.split("\n")[1].split(" ")
        try:
            val[0] = int(val[0]).replace("%", "")
            val[1] = int(val[1])
            val[2] = int(val[2])
        except:
            raise OperationException("Invalid df output when getting quota" \
                " from [%s]\n %s" % (source_full_path, outputs))
        return val

