from Protocol import Protocol

class ProtocolSrmv1(Protocol):

    def __init__(self, SEname, port = "8443", name = "srmv1"):
        Protocol.__init__(self, SEname, port, name)
        self._SEpath = "srm://"+self._SEname+":"+self._port
        self._option = " -debug=true -retry_timeout 480000 -retry_num 3 "

    def getProtocol(self):
        return self._name

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
        if exit_code == 0:
            exit_msg = outputStr

        return exit_code, exit_msg


    def delete(self, filePath, SEhost = None, port = "8443"):
        """
        srm-advisory-delete
        """
        fullSource = ""
        if SEhost is None:
            fullSource = self._SEpath + filePath
        else:
            fullSource = "srm://"+ SEhost +":"+ port + filePath
        option = " -debug=true -retry_num=0"
        cmd = "srm-advisory-delete " +option +" "+ fullSource
        print "executing command: " +str(cmd)
        exit_code, outputStr = self.executeCommand(cmd)
        ### need to parse the output of the commands ###
        print outputStr
        pass

    def checkPermission(self, filePath, SEhost = None, port = "8443"):
        """
        file permission
        """
        size, owner, group, permMode = self.listPath(filePath, SEhost, port)
        return permMode

    def getFileSize(self, filePath, SEhost = None, port = "8443"):
        """
        file size
        """
        ##size, owner, group, permMode = self.listPath(filePath, SEhost, port)
        size = self.listPath(filePath, SEhost, port)[0]
        return int(size)

    def getDirSize(self, fullPath):
        """
        recursively get dir size
        """
        return

    def listPath(self, fullPath, SEhost = None, port = "8443"):
        """
        srm-get-metadata

        returns size, owner, group, permMode of the file-dir
        """
        fullSource = ""
        if SEhost is None:
            fullSource = self._SEpath + fullPath
        else:
            fullSource = "srm://"+ SEhost +":"+ port + fullPath
        option = " -debug=true -retry_num=0"
        cmd = "srm-get-metadata " +option +" "+ fullSource
        print "executing command: " +str(cmd)
        exit_code, outputStr = self.executeCommand(cmd)
        ### need to parse the output of the commands ###
        size, owner, group, permMode = "", "", "", ""
        for line in outputStr:
            if line.find("    size ") != -1:
                size = line.split(":")[1].strip()
            elif line.find("    owner ") != -1:
                owner = line.split(":")[1].strip()
            elif line.find("    group ") != -1:
                group = line.split(":")[1].strip()
            elif line.find("    permMode ") != -1:
                permMode = line.split(":")[1].strip()
        
        return int(size), owner, group, permMode
        

    def checkExists(self, filePath, SEhost = None, port = "8443"):
        """
        file exists?
        """
        size, owner, group, permMode = self.listPath(filePath, SEhost, port)
        if size is not "" and owner is not "" and\
           group is not "" and permMode is not "":
            return True
        return False

# srm-reserve-space srm-release-space srmrmdir srmmkdir srmping srm-bring-online
