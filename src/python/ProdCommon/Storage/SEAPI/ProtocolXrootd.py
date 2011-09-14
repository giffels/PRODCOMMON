"""
Class interfacing with rfio end point
"""

from Protocol import Protocol
from Exceptions import *

class ProtocolXrootd(Protocol):
    """
    implementing the rfio protocol
    """

    def __init__(self):

        super(ProtocolXrootd, self).__init__()


    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text lookng for Exceptions
        """
        problems = []
        lines = []
        if outLines.find("\n") != -1:
            lines = outLines.split("\n")
        for line in lines:
            #print "line = ", line 
            line = line.lower()
            if line.find("network is unreachable") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("stale nfs file handle") != -1 or \
               line.lower().find("killed") != -1:
                raise NFSException("", [line], outLines)
            elif line.find("permission denied") != -1:
                raise AuthorizationException("Permission denied!", \
                                              [line], outLines)
            elif line.find("file exists") != -1:
                raise AlreadyExistsException("File already exists!", \
                                              [line], outLines)
            elif line.find("no such file or directory") != -1 or \
               line.find("error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)
            elif line.find("command not found") != -1:
                raise MissingCommand("Command not found: client not " \
                                     "installed or wrong environment", \
                                     [line], outLines)
            elif line.find("command not recognized") != -1:
                raise MissingCommand("Command not recognized", \
                                     [line], outLines)

        return problems

    
    def createDir(self, dest, token = None, opt = "", tout = None):
        """
        xrd eoscms mkdir /eos...... 
        it could be it is not necessary
        """
        #print "--->>> xrootd: in createDir"
        #print "in createDir dest.workon = ", dest.workon

        if self.checkDirExists(dest, token, opt = "", tout = None) is True:
            problems = ["destination directory already existing", dest.workon]
            raise AlreadyExistsException("Error creating directory [" +\
                                          dest.workon+ "]", problems)

        fullDest = str(dest.getLynk())
        #print "in createDir fullDest = ", fullDest
        
        full_path = "/" + str.split(fullDest,'//')[2]
        #print "in createDir full_path = ", full_path
        
        #opt = 'eoscms '
        opt = str.split(fullDest,'//')[1] + ' '
        #print "---> opt = ", opt
        cmd = "xrd " + opt + "mkdir " + full_path
        #print "in createDir cmd = ", cmd

        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###

        problems = self.simpleOutputCheck(outputs)
        #print "in createDir problems = ", problems
        #print "in createDir outputs = ", outputs
        
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +fullDest+ "].", problems, outputs)
   
    def copy(self, source, dest, token = None, opt = "", tout = None):
        """
        xrdcp
        """
        #print "in copy di ProtocolXrootd.py"
        fullSource = source.workon
        fullDest = dest.workon
        #print "in copy source.workon = ", fullSource
        #print "in copy dest.workon = ", fullDest
        if source.protocol != 'local':
            fullSource = str(source.getLynk())
            #print "in copy source.protocol = ", source.protocol 
            #print "in copy fullSource = ", fullSource 
        if dest.protocol != 'local':
            fullDest = str(dest.getLynk())
            #print "in copy dest.protocol = ", dest.protocol
            #print "in copy fullDest = ", fullDest

        opt = '-np '
        cmd = "xrdcp " + opt + " "+ fullSource +" "+ fullDest 
        print "in copy cmd = ", cmd
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token)#, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolXrootd, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        #print "in copy problems = ", problems
        #print "in copy outputs = ", outputs
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def delete(self, source, token = None, opt = "", tout = None):
        """
        xrd eoscms rm
        """
        #print "in delete di ProtocolXrootd"
        fullSource = str(source.getLynk())
        #print "in delete source.getLynk() = ", fullSource 
        full_path = "/" + str.split(fullSource,'//')[2]
        #print "in delete fullSource = ", fullSource
        #print "in delete full_path = ", full_path
        #opt = 'eoscms '
        opt = str.split(fullSource,'//')[1] + ' '
        #print "---> opt = ", opt
        cmd = "xrd " + opt + "rm "+ full_path
        #print "--->> cmd = ", cmd
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolXrootd, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        #print "in delete problems = ", problems
        #print "in delete outputs = ", outputs
        
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )
    
    def getFileSize(self, source, token = None, opt = "", tout = None):
        """
        _getFileSize_
         xrd eoscms stat  
        """
        #print "in getFileSize di ProtocolXrootd"
        fullSource = str(source.getLynk())
        #print "in getFileSize source.getLynk() = ", fullSource 
        full_path = "/" + str.split(fullSource,'//')[2]
        #print "in getFileSize fullSource = ", fullSource
        #print "in getFileSize full_path = ", full_path
        
        if self.checkExists(source, opt):
            #opt = 'eoscms '
            opt = str.split(fullSource,'//')[1] + ' '
            #print "---> opt = ", opt
            cmd = "xrd " + opt + "stat"+ full_path + " awk '{print $4}' "
            print "in getFileSize cmd = ", cmd
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
    
    def listPath(self, source, token = None, opt = "", tout = None):
        """
        command = xrd eoscms dirList /eos/......
        opt = eoscms 
        
        returns list of files
        """
        #print "--->>> xrootd: in listPath"
        #print "source.workon = ", source.workon
        fullSource = str(source.getLynk())
        #print "source.getLynk() = ", fullSource 
        full_path = "/" + str.split(fullSource,'//')[2]
        #print "fullSource = ", fullSource
        #print "full_path = ", full_path
        #opt = 'eoscms '
        opt = str.split(fullSource,'//')[1] + ' '
        #print "---> opt = ", opt
        cmd = "xrd " + opt + "dirlist "+ full_path
        #print "--->> cmd = ", cmd
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolXrootd, self).executeCommand(cmd)#, timeout = tout)
        #print "----------------------"
        #print "outputs = ", outputs
        #print "exitcode = ", exitcode
        #print "----------------------"

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            if str(problems).find("such file or directory") != -1 or \
               str(problems).find("does not exist") != -1:
                return False
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        #print "---> in listPath source.workon = ", source.workon  
        if token is not None:
            outputs = outputs.split("\n",3)[-1]
            #print "--->>> in token not none" 
            #print "--->>> outputs = ", outputs
        
        outt = [] #outputs.split("\n")
        import os
        for line in outputs.split("\n"):
            try:
                line = line.split()[-1]
            except: continue    
            outt.append( os.path.join( line ) )
            #print "outt = ", outt

        return outt
        
    def checkExists(self, source, token = None, opt = "", tout = None):
        """
        #file exists?
        """
        #print "--->>> xrootd: in checkExists"
        fullSource = str(source.getLynk())
        #print "in checkExists source.getLynk() = ", fullSource 
        full_path = "/" + str.split(fullSource,'//')[2]
        #print "in checkExists fullSource = ", fullSource
        #print "in checkExists full_path = ", full_path
        #opt = 'eoscms '
        opt = str.split(fullSource,'//')[1] + ' '
        #print "---> opt = ", opt
        cmd = "xrd " + opt + "stat " + full_path 
        #print "in checkExists cmd = ",cmd
        exitcode, outputs = self.executeCommand(cmd, timeout = tout)
        #print "in checkExists exitcode = ", exitcode
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0:
            return False

        for problema in problems:
            if "no such file or directory" in problema:
                return False
        return True

    def checkDirExists(self, source, token = None, opt = "", tout = None):
        """
        xrd eoscms stat /eos/....
        """
        #print "--->>> xrootd: in checkDirExists"
        
        fullSource = str(source.getLynk())
        #print "in checkDirExists source.getLynk() = ", fullSource 
        full_path = "/" + str.split(fullSource,'//')[2]
        #print "in checkDirExists fullSource = ", fullSource
        #print "in checkDirExists full_path = ", full_path
        
        #opt = 'eoscms '
        opt = str.split(fullSource,'//')[1] + ' '
        #print "---> opt = ", opt
        cmd = "xrd " + opt + "stat " + fullSource
        #print "in checkDirExists cmd = ",cmd
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)
        problems = self.simpleOutputCheck(outputs)
        #print "in checkDirExists problems = ", problems
        #print "in checkDirExists outputs = ", outputs
        for problema in problems:
            if "no such file or directory" in problema:
                return False
        if exitcode == 0:
            return True
        else:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
