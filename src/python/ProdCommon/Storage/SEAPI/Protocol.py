from Exceptions import OperationException
import logging

class Protocol(object):
    '''Represents any Protocol'''

    def __init__(self, logger = None):
        self.logger = logger
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

    def deleteRec(self, source):
        """
        delete dir and subdir/file
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

    def setGrant(self, source, value):
        """
        set permissions on the specified path
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

    def checkUserProxy( self, cert='' ):
        """
        Retrieve the user proxy for the task
        If the proxy is valid pass, otherwise raise an axception
        """

        command = 'voms-proxy-info'

        if cert != '' :
            command += ' --file ' + cert
        else:
            import os
            command += ' --file ' + str(os.environ['X509_USER_PROXY'])

        status, output = self.executeCommand( command )

        if status != 0:
            raise OperationException("Missing Proxy", "Missing Proxy")

        try:
            output = output.split("timeleft  :")[1].strip()
        except IndexError:
            raise OperationException("Missing Proxy", "Missing Proxy")

        if output == "0:00:00":
            raise OperationException("Proxy Expired", "Proxy Expired")

    def makeNonBlocking(self, fd):
        """
        _makeNonBlocking_
        """
        import os, fcntl
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        try:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
        except AttributeError:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.FNDELAY)

    def executeCommand(self, command):
        """
        common method to execute commands
 
        return exit_code, cmd_out
        """
        import commands
        status, output = commands.getstatusoutput( command )
        self.__logout__(str(command), str(status), str(output))
        return status, output

    def __logout__(self, command, status, output):
        """
        write to log file
        """
        if self.logger == None:
            logfile = "./.SEinteraction.log"
            import datetime
            tow = "Executed:\t%s\nDone with exit code:\t%s\nand output:\n%s\n"%(command,status,output)
            writeout = str(datetime.datetime.now()) + ":\n" + str(tow) + "\n"
            file(logfile, 'a').write(writeout)
        else:
            self.logger.debug("Command:\t%s"%command)
            self.logger.debug("ExitCode:\t%s"%status)
            self.logger.debug("Output:\t%s"%output)

