
class Protocol(object):
    '''Represents any Protocol'''

    def __init__(self, SEname, port, name):
        self._name = str.lower(name)
        self._SEname = str.lower(SEname)
        self._port = port

    def move(self, source, dest):
        """
        move a file from a source to a dest
 
        return string
        """
        return

    def copy(self, source, dest):
        """
        copy a file from a source to a dest
 
        return string
        """
        raise NotImplementedError

    def delete(self, filePath):
        """
        delete a file (or a path)
 
        return string
        """
        raise NotImplementedError

    def checkPermission(self, filePath):
        """
        get the permission of a file/path in number value
 
        return int
        """
        return

    def getFileSize(self, filePath):
        """
        get the file size
 
        return int
        """
        return
 
    def getDirSize(self, fullPath):
        """
        get the directory size
        (considering subdirs and files)
 
        return int
        """
        return
 
    def listPath(self, fullPath):
        """
        list the content of a path
 
        return list[string]
        """
        return
 
    def checkExists(self, filePath):
        """
        check if a file exists
 
        return bool
        """
        return
 
    def getGlobalQuota(self):
        """
        get the global occupated space %,
                       free quota,
                       occupated quota
 
        return [int, int, int]
        """
        return
 
    def executeCommand(self, command ):
        """
        common method to execute commands
 
        return exit_code, cmd_out
        """
        import commands
 
        status, output = commands.getstatusoutput( command )
        return status, output

