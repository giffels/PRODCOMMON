
class Protocol(object):
    '''Represents any Protocol'''

    def __init__(self):
        pass

    def move(self, source, sest):
        """
        move a file from a source to a dest
        """
        raise NotImplementedError

    def copy(self, source, dest):
        """
        copy a file from a source to a dest
        """
        raise NotImplementedError

    def delete(self, source):
        """
        delete a file (or a path)
        """
        raise NotImplementedError

    def checkPermission(self, source):
        """
        get the permission of a file/path in number value
 
        return int
        """
        raise NotImplementedError

    def createDir(self, source):
        """
        create a directory
        """
        raise NotImplementedError

    def getFileSize(self, source):
        """
        get the file size
 
        return int
        """
        raise NotImplementedError
 
    def getDirSize(self, source):
        """
        get the directory size
        (considering subdirs and files)
 
        return int
        """
        raise NotImplementedError
 
    def listPath(self, source):
        """
        list the content of a path
 
        return list[string]
        """
        raise NotImplementedError
 
    def checkExists(self, source):
        """
        check if a file exists
 
        return bool
        """
        raise NotImplementedError
 
    def getGlobalQuota(self, source):
        """
        get the global occupated space %,
                       free quota,
                       occupated quota
 
        return [int, int, int]
        """
        raise NotImplementedError
 
    def executeCommand(self, command):
        """
        common method to execute commands
 
        return exit_code, cmd_out
        """
        import commands
        status, output = commands.getstatusoutput( command )
        return status, output

