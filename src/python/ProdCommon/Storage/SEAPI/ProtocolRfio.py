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
        lines = outLines.split("\n")
        for line in lines:
            if line.find("Network is unreachable") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("Permission denied") != -1:
                raise AuthorizationException("Permission denied!", \
                                              [line], outLines)
            elif line.find("File exists") != -1:
                raise AlreadyExistsException("File already exists!", \
                                              [line], outLines)
            elif line.find("No such file or directory") != -1 or \
               line.find("error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                   [], outLines)
        return problems


    def setGrant(self, dest, values, token = None, opt = ""):
        """
        rfchomd
        """
        
        fullDest = dest.getLynk()

        cmd = "rfchmod " + opt + " " + str(values) + " " + fullDest
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error changing permission... " + \
                                    "[" +fullDest+ "].", problems, outputs)


    def createDir(self, dest, token = None, opt = ""):
        """
        rfmkdir
        """
        if self.checkDirExists(dest, token, opt = "") is True:
            problems = ["destination directory already existing", dest.workon]
            raise AlreadyExistsException("Error creating directory [" +\
                                          dest.workon+ "]", problems)

        fullDest = dest.getLynk()

        cmd = "rfmkdir -p " + opt + " " + fullDest 
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +fullDest+ "].", problems, outputs)

    def copy(self, source, dest, token = None, opt = ""):
        """
        rfcp
        """
        fullSource = source.workon
        fullDest = dest.workon
        if source.protocol != 'local':
            fullSource = source.getLynk()
        if dest.protocol != 'local':
            fullDest = dest.getLynk()

        cmd = "rfcp " + opt + " "+ fullSource +" "+ fullDest 
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, token = None, opt = ""):
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

    def deleteRec(self, source, token = None, opt = ""):
        """
        _deleteRec_
        """
        self.delete(source, token, opt)

    def delete(self, source, token = None, opt = ""):
        """
        rfrm
        """
        fullSource = source.getLynk()

        cmd = "rfrm " + opt + " "+ fullSource
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def getFileInfo(self, source, token = None, opt = ""):
        """
        rfdir

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        cmd = "rfdir " + opt + " " + fullSource + " | awk '{print $5,$3,$4,$1}'"
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        
        if token is not None: 
            outputs = outputs.split("\n",3)[-1]
        
 
        outt = []
        for out in outputs.split("\n"):
            fileout = out.split()
            fileout[3] = self.__convertPermission__(out[3])
            outt.append( fileout ) 
        ### need to parse the output of the commands ###
        
        return outt


    def checkPermission(self, source, token = None, opt = ""):
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

    def getFileSize(self, source, token = None, opt = ""):
        """
        file size
        """
        result = self.getFileInfo(source, token, opt)
        if result.__type__ is list:
            if result[0].__type__ is list:
                raise OperationException("Error: Not empty directory given!")
            else:  
                return result[0]
        else:
            raise OperationException("Error: Not empty directory given!")

        return int(result)

    def listPath(self, source, token = None, opt = ""):
        """
        rfdir

        returns list of files
        """
        fullSource = source.getLynk()

        cmd = "rfdir " + opt + " "+ fullSource +" | awk '{print $9}'"
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)
        
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )

        if token is not None:
            outputs = outputs.split("\n",3)[-1]

        outt = [] #outputs.split("\n")
        import os
        for line in outputs.split("\n"):
            outt.append( os.path.join( source.getFullPath(), line ) )

        return outt
        

    def checkExists(self, source, token = None, opt = ""):
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


    def checkDirExists(self, source, token = None, opt = ""):
        """
        rfstat
        note: rfdir prints nothing if dir is empty
        returns boolean 
        """
        fullSource = source.getLynk()

        cmd = "rfstat " + opt + " " + fullSource
        exitcode, outputs = None, None
        if token is not None:
            exitcode, outputs = self.executeCommand( cmd, token )
        else:
            exitcode, outputs = super(ProtocolRfio, self).executeCommand(cmd)
        problems = self.simpleOutputCheck(outputs)
        for problema in problems:
            if "No such file or directory" in problema:
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

    def executeCommand(self, cmd, token):
        """
        execute the command passing by ksu (file input mode)
        """
        import tempfile
        import os

        userName, token = token.split('::')#will be removed with next api version
        BaseCmd = self.ksuCmd +'/usr/kerberos/bin/ksu %s -k -c FILE:%s < '%(userName,token)
        exit, out = None, None
        fname = None
        try:
            tmp, fname = tempfile.mkstemp( "", "ksu_", os.getcwd() )
            os.close( tmp )
            file(fname, 'w').write( cmd + "\n" )

            command = BaseCmd + fname
            self.__logout__("Executing through ksu:\t" + str(cmd) + "\n")
            #from ProdCommon.BossLite.Common.System import executeCommand
            exit, out = super(ProtocolRfio, self).executeCommand(command)
            #out, exit = executeCommand(command)
        finally:
            os.unlink( fname )

        return exit, out
