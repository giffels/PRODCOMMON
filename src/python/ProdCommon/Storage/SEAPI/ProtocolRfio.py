from Protocol import Protocol
from Exceptions import *

class ProtocolRfio(Protocol):
    """
    implementing the rfio protocol
    """

    def __init__(self):
        super(ProtocolRfio, self).__init__()

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text lookng for Exceptions
        """
        problems = []
        lines = outLines.split("\n")
        for line in lines:
            if line.find("No such file or directory") != -1 or \
               line.find("error") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", [], outLines)
        return problems


    def setGrant(self, dest, values, opt = ""):
        """
        rfchomd
        """
        
        fullDest = dest.getLynk()
        cmd = "rfchmod " + opt + " " + str(values) + " " + fullDest
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error changing permission... " + \
                                    "[" +fullDest+ "].", problems, outputs)


    def createDir(self, dest, opt = ""):
        """
        rfmkdir
        """

        if self.checkExists(dest, opt = ""):
            problems = ["destination directory already existing", dest.workon]
            raise OperationException("Error creating directory [" +\
                                      dest.workon+ "]", problems)
        
        fullDest = dest.getLynk()

        cmd = "rfmkdir -p " + opt + " " + fullDest 
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +fullDest+ "].", problems, outputs)

    def copy(self, source, dest, proxy = None, opt = ""):
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
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None, opt = ""):
        """
        copy() + delete()
        """
        if self.checkExists(dest, opt):
            problems = ["destination file already existing", dest.workon]
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems)
        self.copy(source, dest, opt)
        if self.checkExists(dest, opt):
            self.delete(source, opt)
        else:
            raise TransferException("Error deleting [" +source.workon+ "]", \
                                     ["Uknown Problem"] )

    def deleteRec(self, source, opt = ""):
        self.delete(source, opt)

    def delete(self, source, opt = ""):
        """
        rfrm
        """
        fullSource = source.getLynk()

        cmd = "rfrm " + opt + " "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def getFileInfo(self, source, opt = ""):
        """
        rfdir

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        cmd = "rfdir " + opt + " " + fullSource + " | awk '{print $5,$3,$4,$1}'"
        exitcode, outputs = self.executeCommand(cmd)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        outt = outputs.split()
        outt[3] = self.__convertPermission__(outt[3])
        ### need to parse the output of the commands ###
        
        return outt


    def checkPermission(self, source, opt = ""):
        """
        return file/dir permission
        """
        return self.getFileInfo(source, opt)[3]

    def getFileSize(self, source, opt = ""):
        """
        file size
        """
        size = self.getFileInfo(source, opt)[0]
        return int(size)

    def listPath(self, source, opt = ""):
        """
        rfdir

        returns list of files
        """
        fullSource = source.getLynk()

        cmd = "rfdir " + opt + " "+ fullSource +" | awk '{print $9}'"
        exitcode, outputs = self.executeCommand(cmd)
        
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        outt = outputs.split("\n")
        ### need to parse the output of the commands ###
        return outt
        

    def checkExists(self, source, opt = ""):
        """
        file exists?
        """
        try:
            size, owner, group, permMode = self.getFileInfo(source, opt)
            if size is not "" and owner is not "" and\
               group is not "" and permMode is not "":
                return True
        except NotExistsException:
            return False
        except OperationException:
            return False

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

