"""
Class interfacing with LStore
  - Andrew Melo <andrew.m.melo@vanderbilt.edu>
"""
import traceback
from Protocol import Protocol
from Exceptions import * 
import os, os.path

class ProtocolLStore(Protocol):
    """
    implementing a "local protocol", using unix system commmands
    """
 
    def __init__(self):
        self.scriptPath = "/usr/local/cms-stageout"
        super(ProtocolLStore, self).__init__()

    def _transfer(self, source, dest, opt = "", proxy="", tout = None, delete=False):
        """
        move from source.workon to dest.workon, optionally deleting
        """
        exitcode = -1
        outputs = ""
        ll = dest.getLynk()
        if not ll.startswith('srm://se1.accre.vanderbilt.edu'):
            # don't let any of this happen if the output's not going
            # to vanderbilt
            raise TransferException("WARNING: Vandy stageout stuff was "+\
                            "called, but files aren't going to vandy")

        dest_fullpath = self._stripPath( ll ) 
        
        if self.checkExists(source, opt):
            command = os.path.join( self.scriptPath, "vandyCp.sh" )
            command = " ".join( [command, source.workon, dest_fullpath] )
            if proxy:
                command = "X509_USER_PROXY=%s %s" % (proxy, command)
            print "executing %s" % command
            exitcode, outputjs = self.executeCommand(command, 
                                                        timeout = tout)
            print outputjs
            if delete:
                os.unlink( source.workon )
        else:
            raise NotExistsException("Error: path [" + source.workon + \
                                     "] does not exists.")
        if exitcode != 0:
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    +dest_fullpath+ "]\n " +outputs)
 
    def copy(self, source, dest, proxy = "",opt = "", tout = None):
        """
        copy from source.workon to dest.workon
        """
        return self._transfer(source, dest, opt=opt,tout=tout,delete=False,\
                                proxy=proxy)

    def move(self, source, dest, proxy="", opt = "", tout = None):
        """
        move from source.workon to dest.workon
        """
        return self._transfer(source, dest, opt=opt,tout=tout,delete=True,\
                                proxy=proxy)



    def delete(self, source, proxy="", opt = "", tout = None):
        """
        _delete_
        """
        exitcode = -1
        outputs = ""
        ll = source.getLynk()
        if not ll.startswith('srm://se1.accre.vanderbilt.edu'):
            # don't let any of this happen if the output's not going
            # to vanderbilt
            raise TransferException("WARNING: Vandy stageout stuff was "+\
                            "called, but files aren't going to vandy")


        source_fullpath = ll.split("lstore://",1)[1]
        if self.checkExists(source, opt):
            cmd = " ".join( os.path.join(self.scriptPath, "vandyRm.sh"),
                            opt,
                            source_fullpath )
            if proxy:
                cmd = "X509_USER_PROXY=%s %s" % (proxy, opt)
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
        if not ll.startswith('srm://se1.accre.vanderbilt.edu'):
            # don't let any of this happen if the output's not going
            # to vanderbilt
            raise TransferException("WARNING: Vandy stageout stuff was "+\
                            "called, but files aren't going to vandy")


        source_fullpath = self._stripPath(ll)
        if not self.checkExists(source, opt):
            cmd = " ".join([os.path.join(self.scriptPath, "vandyMkdir.sh"),
                            opt,
                            source_fullpath])
            print "Executing %s" % cmd
            exitcode, outputs = self.executeCommand(cmd, timeout = tout)
            if exitcode != 0:
                raise OperationException("Error creating [" +source_fullpath \
                                             + "]\n "+outputs)
        else:
            print "directory" + source_fullpath + "already exists"
 
    def checkPermission(self, source, opt = "", tout = None):
        """
        _checkPermission_
        """
        exitcode = -1
        outputs = ""
        ll = source.getLynk()
        source_fullpath = ll.split("lstore://",1)[1]
        if self.checkExists(source, opt):
            outputs = '777'
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
    
    def _stripPath(self, ll):
        if ll.find("lstore://") != -1:
            fullpath = ll.split("lstore://",1)[1] 
        elif ll.find("file://") != -1:
            fullpath = ll.split("file://",1)[1]
        elif ll.find("srm://se1.accre.vanderbilt.edu") != -1:
            fullpath = ll.split("SFN=",1)[1]
        else:
            fullpath = ll
        return fullpath


    def checkExists(self, source, proxy="", opt = "", tout = None):
        """
        _checkExists_
        """
        ll = source.getLynk()
        fullpath = self._stripPath(ll)
        out = os.path.exists( fullpath )
        return out
 
    def getGlobalQuota(self, source, opt = "", tout = None):
        """
        _getGlobalQuota_

        lstore dosnt have quotas
        """
        return "9999999999"
