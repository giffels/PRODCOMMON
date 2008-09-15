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
            if line.find("No such file or directory") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
        return problems


    def setGrant(self, dest, values):
        """
        rfchomd
        """
        
        fullDest = dest.getLynk()
        cmd = "rfchmod " + str(values) + " " + fullDest
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error changing permission... " + \
                                    "[" +fullDest+ "].", problems, outputs)


    def createDir(self, dest):
        """
        rfmkdir
        """

        if self.checkExists(dest):
            problems = ["destination file already existing", dest.workon]
            raise OperationException("Error creating directory [" +\
                                      dest.workon+ "]", problems, outputs )
        
        fullDest = dest.getLynk()

        cmd = "rfmkdir -p " + fullDest 
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +fullDest+ "].", problems, outputs)

    def copy(self, source, dest, proxy = None):
        """
        rfcp
        """
        fullSource = source.workon
        fullDest = dest.workon
        if source.protocol != 'local':
            fullSource = source.getLynk()
        if dest.protocol != 'local':
            fullDest = dest.getLynk()

        cmd = "rfcp "+ fullSource +" "+ fullDest
        exitcode, outputs = self.executeCommand(cmd)
        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error copying [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems, outputs )

    def move(self, source, dest, proxy = None):
        """
        copy() + delete()
        """
        if self.checkExists(dest):
            problems = ["destination file already existing", dest.workon]
            raise TransferException("Error moving [" +source.workon+ "] to [" \
                                    + dest.workon + "]", problems)
        self.copy(source, dest)
        if self.checkExists(dest):
            self.delete(source)
        else:
            raise TransferException("Error deleting [" +source.workon+ "]", \
                                     ["Uknown Problem"] )

    def deleteRec(self, source):
        self.delete(source)

    def delete(self, source):
        """
        rfrm
        """
        fullSource = source.getLynk()

        cmd = "rfrm "+ fullSource
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def getFileInfo(self, source):
        """
        rfdir

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = source.getLynk()

        cmd = "rfdir "+ fullSource +" | awk '{print $5,$3,$4,$1}'"
        exitcode, outputs = self.executeCommand(cmd)

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        outt = outputs.split()
        outt[3] = self.__convertPermission__(outt[3])
        ### need to parse the output of the commands ###
        
        return outt


    def checkPermission(self, source):
        """
        return file/dir permission
        """
        return self.getFileInfo(source)[3]

    def getFileSize(self, source):
        """
        file size
        """
        size = self.getFileInfo(source)[0]
        return int(size)

    def listPath(self, source):
        """
        rfdir

        returns list of files
        """
        fullSource = source.getLynk()

        cmd = "rfdir "+ fullSource +" | awk '{print $9}'"
        exitcode, outputs = self.executeCommand(cmd)
        
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error reading [" +source.workon+ "]", \
                                      problems, outputs )
        outt = outputs.split("\n")
        ### need to parse the output of the commands ###
        return outt
        

    def checkExists(self, source):
        """
        file exists?
        """
        try:
            print "\n\n\n " +str(self.getFileInfo(source))+ "\n\n\n\n"
            size, owner, group, permMode = self.getFileInfo(source)
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

