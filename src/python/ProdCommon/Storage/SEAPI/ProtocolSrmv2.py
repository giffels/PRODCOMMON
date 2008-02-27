from Protocol import Protocol

class ProtocolSrmv2(Protocol):

    def __init__(self, SEname, port = "8443", name = "srmv2"):
        Protocol.__init__(self, SEname, port, name)
        self._SEpath = "srm://"+self._SEname+":"+self._port
        self._option = " -2 -debug=true -retry_timeout 480000 -retry_num 3 "

    def getProtocol(self):
        return self._name

    def move(self, source, dest, type = 1, SEhost = None, port = "8443", protocol = "srm"):
        """
        srmmv
        """
        fullSource = ""
        fullDest = ""

        exit_code = -1
        exit_msg = ""

        if type == 1:
            #transf:  this -> remoteSE
            fullSource = self._SEpath + source
            if SEhost != None and port != None and protocol != None:
                fullDest = protocol +"://"+ SEhost +":"+ port + dest
            else:
                exit_msg  = "Missing the SEhost parameters"
        elif type == 2:
            #transf:  remoteSE -> this
            fullSource = protocol +"://"+ SEhost +":"+ port + source
            fullDest = self._SEpath + source
        else:
            exit_msg  = "Type of transfer not supported (choose between 1-2)"

        option = self._option + ""

        cmd = "srmmv " +option +" "+ fullSource +" "+ fullDest
        print "executing command: " +str(cmd)
        exit_code, outputStr = self.executeCommand(cmd)

        ### need to parse the output of the commands ###

        return exit_code, exit_msg


    def copy(self, source, dest, type = 1, SEhost = None, port = "8443", protocol = "srm"):
        """
        srmcp
        """
        fullSource = ""
        fullDest = ""

        exit_code = -1
        exit_msg = ""

        if type == 1:
            #transf:  this -> remoteSE
            fullSource = self._SEpath + source
            if SEhost != None and port != None and protocol != None:
                fullDest = protocol +"://"+ SEhost +":"+ port + dest
            else:
                exit_msg  = "Missing the SEhost parameters"
        elif type == 2:
            #transf:  remoteSE -> this
            fullSource = protocol +"://"+ SEhost +":"+ port + source
            fullDest = self._SEpath + source
        elif type == 3:
            #transf:  path -> this
            fullSource = "file:///"+ source
            fullDest = self._SEpath + dest
        elif type == 4:
            #transf: this -> path
            fullSource = self._SEpath + source
            fullDest = "file:///"+ dest
        else:
            exit_msg  = "Type of transfer not supported (choose between 1-4)"

        option = self._option + ""

        cmd = "srmcp " +option +" "+ fullSource +" "+ fullDest
        print "executing command: " +str(cmd)
        exit_code, outputStr = self.executeCommand(cmd)

        ### need to parse the output of the commands ###

        return exit_code, exit_msg


    def delete(self, filePath):
        """
        srmrm
        """

    def checkPermission(self, filePath):
        return

    def getFileSize(self, filePath):
        return

    def getDirSize(self, fullPath):
        return

    def listPath(self, fullPath, SEhost, port = "8443"):
        """
        srmls
        """
        fullSource = self._SEpath + fullPath
        option = self._option + ""
        cmd = "srmls " +option +" "+ fullSource
        print "executing command: " +str(cmd)
        exit_code, outputStr = self.executeCommand(cmd)

        ### need to parse the output of the commands ###

        return exit_code, outputStr

    def checkExists(self, filePath, SEhost, port = "8443"):
        """
        file exists?
        """
        exit_code, outputStr = self.listPath(self, filePath, SEhost, port)

        ### need to parse the output of the commands ###
        if exit_code != 0:
            return False
        return True

# srm-reserve-space srm-release-space srmrmdir srmmkdir srmping srm-bring-online
