"""
Class interfacing with rfio end point
"""

from Protocol import Protocol
from Exceptions import *

class ProtocolRfio(Protocol):
    """
    implementing the rfio protocol
    """

    def __init__(self):
        super(ProtocolRfio, self).__init__()
        self.ksuCmd = ' cd /tmp; unset LD_LIBRARY_PATH; export PATH=/usr/bin:/bin; source /etc/profile; '
        self.ksuOut = [ \
                        "Authenticated ", \
                        "Acount ", \
                        "authorization for ", \
                        "Changing uid to "
                      ]


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

        return problems


    def setGrant(self, dest, values, token = None, opt = "", tout = None):
        """
        rfchomd
        """
        
        fullDest = dest.getLynk()

        if str.find(str(fullDest),'path=') != -1:
            fullDest = str.split(str(fullDest),'path=')[1]
        cmd = "rfchmod " + opt + " " + str(values) + " " + fullDest
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error changing permission... " + \
                                    "[" +fullDest+ "].", problems, outputs)


    def createDir(self, dest, token = None, opt = "", tout = None):
        """
        rfmkdir
        """
        if self.checkDirExists(dest, token, opt = "", tout = None) is True:
            problems = ["destination directory already existing", dest.workon]
            raise AlreadyExistsException("Error creating directory [" +\
                                          dest.workon+ "]", problems)

        fullDest = dest.getLynk()
        if str.find(str(fullDest),'path=') != -1:
            fullDest = str.split(str(fullDest),'path=')[1]

        cmd = "rfmkdir -p " + opt + " " + fullDest 
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +fullDest+ "].", problems, outputs)

    def copy(self, source, dest, token = None, opt = "", tout = None):
        """
        rfcp
        """
        fullSource = source.workon
        fullDest = dest.workon
        if source.protocol != 'local':
            fullSource = source.getLynk()
        if dest.protocol != 'local':
            fullDest = dest.getLynk()
            if str.find(str(fullDest),'path=') != -1:
                fullDest = str.split(str(fullDest),'path=')[1]

        cmd = "rfcp " + opt + " "+ fullSource +" "+ fullDest 
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token)#, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, token = None, opt = "", tout = None):
        """
        copy() + delete()
        """
        if self.checkExists(dest, token, opt):
            problems = ["destination file already existing", dest.workon]
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems)
        self.copy(source, dest, token, opt)
        if self.checkExists(dest, token, opt):
            self.delete(source, token, opt)
        else:
            raise TransferException("Error deleting [" +source.workon+ "]", \
                                     ["Uknown Problem"] )

    def deleteRec(self, source, token = None, opt = "", tout = None):
        """
        _deleteRec_
        """
        self.delete(source, token, opt)

    def delete(self, source, token = None, opt = "", tout = None):
        """
        rfrm
        """
        fullSource = source.getLynk()

        if str.find(str(fullSource),'path=') != -1:
            fullSource = str.split(str(fullSource), 'path=')[1]

        cmd = "rfrm " + opt + " "+ fullSource
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def getFileInfo(self, source, token = None, opt = "", tout = None):
        """
        rfdir

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()
        if str.find(str(fullSource),'path=') != -1:
            fullSource = str.split(str(fullSource),'path=')[1]

        cmd = "rfdir " + opt + " " + fullSource + " | awk '{print $5,$3,$4,$1}'"
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token)#, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        
        if token is not None: 
            outputs = outputs.split("\n",3)[0]
        
 
        outt = []
        for out in outputs.split("\n"):
            if out:
               fileout = out.split()
               fileout[3] = self.__convertPermission__(out[3])
               outt.append( fileout ) 
        ### need to parse the output of the commands ###
        
        return outt


    def checkPermission(self, source, token = None, opt = "", tout = None):
        """
        return file/dir permission
        """
        result = self.getFileInfo(source, token, opt)
        if result.__type__ is list:
            if result[0].__type__ is list:
                raise OperationException("Error: Not empty directory given!")
            else:
                return result[3]
        else:
            raise OperationException("Error: Not empty directory given!")

    def getFileSize(self, source, token = None, opt = "", tout = None):
        """
        file size
        """
        result = self.getFileInfo(source, token, opt)
        try:
            return int(result[0][0])
        except:
            raise OperationException("Error: Not empty directory given!")


    def listPath(self, source, token = None, opt = "", tout = None):
        """
        rfdir

        returns list of files
        """
        fullSource = source.getLynk()
        if str.find(str(fullSource),'path=') != -1:
            fullSource = str.split(str(fullSource),'path=')[1]

        cmd = "rfdir " + opt + " "+ fullSource +" | awk '{print $9}'"
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)
        
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            if str(problems).find("such file or directory") != -1 or \
               str(problems).find("does not exist") != -1:
                return False
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )

        if token is not None:
            outputs = outputs.split("\n",3)[-1]

        outt = [] #outputs.split("\n")
        import os
        for line in outputs.split("\n"):
            outt.append( os.path.join( source.getFullPath(), line ) )

        return outt
        

    def checkExists(self, source, token = None, opt = "", tout = None):
        """
        file exists?
        """
        try:
            for filet in self.getFileInfo(source, token, opt):
                size = filet[0]
                owner = filet[1]
                group = filet[2]
                permMode = filet[3]
                if size is not "" and owner is not "" and\
                   group is not "" and permMode is not "":
                    return True
        except NotExistsException:
            return False
        except OperationException:
            return False
        return False


    def checkDirExists(self, source, token = None, opt = "", tout = None):
        """
        rfstat
        note: rfdir prints nothing if dir is empty
        returns boolean 
        """
        fullSource = source.getLynk()
        if str.find(str(fullSource),'path=') != -1:
            fullSource = str.split(str(fullSource),'path=')[1]

        cmd = "rfstat " + opt + " " + fullSource
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token, timeout = tout )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)#, timeout = tout)
        problems = self.simpleOutputCheck(outputs)
        for problema in problems:
            if "no such file or directory" in problema:
                return False
        if exitcode == 0:
            return True
        else:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )


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

    def executeCommand(self, cmd, token, tout = None):
        """
        execute the command passing by ksu (file input mode)
        """
        import tempfile
        import os

        if token.find('::')> 1:
            userName, token = token.split('::')#will be removed with next api version
        else:  
            userName = token.split('_')[1]#will be removed with next api version
        BaseCmd = self.ksuCmd +'/usr/kerberos/bin/ksu %s -k -c FILE:%s < '%(userName,token)
        exit, out = None, None
        fname = None
        try:
            tmp, fname = tempfile.mkstemp( "", "ksu_", os.getcwd() )
            os.close( tmp )
            file(fname, 'w').write( cmd + "\n" )

            command = BaseCmd + fname
            #from ProdCommon.BossLite.Common.System import executeCommand
            exit, out = super(ProtocolRfio, self).executeCommand(command)#, timeout = tout)
            self.__logout__("Executing through ksu:\t" + str(cmd) + "\n",exit,out)
            #out, exit = executeCommand(command)

            tries = 0
            while (exit != 0 and tries < 5):
                exit, out = super(ProtocolRfio, self).executeCommand(command)#, timeout = tout)
                self.__logout__("Executing through ksu:\t" + str(cmd) + \
                  ": try number " + str(tries) + "\n", exit, out)
                tries += 1

        finally:
            os.unlink( fname )
        return exit, out
