"""
Protocol that makes usage of lcg-utils to make operation with
 srm and gridftp endpoint
"""

from Protocol import Protocol
from Exceptions import *

class ProtocolGlobusUtils(Protocol):
    """
    implementing storage interaction with lcg-utils 
    """

    def __init__(self):
        super(ProtocolGlobusUtils, self).__init__()
        # set verbose option
        self.options = " -vb " 

    def simpleOutputCheck(self, outLines):
        """
        parse line by line the outLines text looking for Exceptions
        """
        problems = []
        lines = outLines.split("\n")
        for line in lines:
            line = line.lower()
            if line.find("no entries for host") != -1 or\
               line.find("srm client error") != -1:
                raise MissingDestination("Host not found!", [line], outLines)
            elif line.find("user has no permission") != -1 or\
                 line.find("permission denied") != -1:
                raise AuthorizationException("Permission denied!", \
                                              [line], outLines)
            elif line.find("file exists") != -1:
                raise AlreadyExistsException("File already exists!", \
                                              [line], outLines)
            elif line.find("no such file or directory") != -1 or \
               line.find("error") != -1 or line.find("failed") != -1 or \
               line.find("cacheexception") != -1 or \
               line.find("does not exist") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
            elif line.find("unknown option") != -1 or \
                 line.find("unrecognized option") != -1 or \
                 line.find("invalid option") != -1:
                raise WrongOption("Wrong option passed to the command", \
                                  [line], outLines)
            elif line.find("command not found") != -1: 
                raise MissingCommand("Command not found: client not " \
                                     "installed or wrong environment", \
                                     [line], outLines)
                
        return problems

    def lessSimpleOutputCheck(self, outLines, sources):
        """
        parse line by line the outLines text looking for Exceptions
        """ 

        errcodeList = ["0"]*len(sources)
        problems = [] 
        problemsList = [""]*len(sources)
        sourceIndex = 0

        lines = outLines.split("\n")
        for line in lines:
            line = line.lower()
             # determine which source in the list the output
             # is going to be for.  Errors for that source will be 
             # added into the correct element in the problems list
            if line.find("source") != -1:
                sourceIndex += 1


            if line.find("no entries for host") != -1 or\
               line.find("srm client error") != -1:
                #raise MissingDestination("Host not found!", [line], outLines)
                problemsList[sourceIndex] += "Host not found! \n" + line
                errcodeList[sourceIndex] = 1

            elif line.find("user has no permission") != -1 or\
                 line.find("permission denied") != -1:
                #raise AuthorizationException("Permission denied!", \
                #                              [line], outLines)
                problemsList[sourceIndex] += "Permission denied! \n" + line
                errcodeList[sourceIndex] = 2

            elif line.find("file exists") != -1:
                #raise AlreadyExistsException("File already exists!", \
                #                              [line], outLines)
                problemsList[sourceIndex] += "File already exists! \n" + line
                errcodeList[sourceIndex] = 3

            elif line.find("no such file or directory") != -1 or \
               line.find("error") != -1 or line.find("failed") != -1 or \
               line.find("cacheexception") != -1 or \
               line.find("does not exist") != -1:
                cacheP = line.split(":")[-1]
                if cacheP not in problems:
                    problems.append(cacheP)
                    problemsList[sourceIndex] += cacheP
                    errcodeList[sourceIndex] = 4

            elif line.find("unknown option") != -1 or \
                 line.find("unrecognized option") != -1 or \
                 line.find("invalid option") != -1:
                #raise WrongOption("Wrong option passed to the command", \
                #                  [line], outLines)
                problemsList[sourceIndex] += "Wrong option passed to the command\n" + line
                errcodeList[sourceIndex] = 5

            elif line.find("command not found") != -1:
                #raise MissingCommand("Command not found: client not " \
                #                     "installed or wrong environment", \
                #                     [line], outLines)
                problemsList[sourceIndex] += "Command not found: client not installed or wrong environment\n" + line
                errcodeList[sourceIndex] = 6

        # errors are printed before the word "Source" comes up, so
        # the sourceIndex is actually the number of sources that were
        # encountered.  So it maps directly to index starting from zero.

        if sourceIndex < (len(sources)):
            while sourceIndex < (len(sources)):
                problemsList[sourceIndex] += "copy not attempted"
                errcodeList[sourceIndex] = -1
                sourceIndex += 1

        problemsTuple = []
        for t in map(None, errcodeList, problemsList):
            problemsTuple.append(t)#[0] + "\n " + t[1])

        return problemsTuple


    def createDir(self, source, proxy = None, opt = ""):
        """
        Uberftp -> mkdir
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"mkdir %s\" " % (source.hostname, os.path.join("/", source.workon))

        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###

        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise TransferException("Error creating remote dir " + \
                                    "[" +source.workon+ "].", problems, outputs)

    def copy(self, source, dest, proxy = None, opt = ""):
        """
        globus-url-copy
        """

        from os.path import join
        from types import StringType, UnicodeType
        import tempfile
        import os

        # sources list
        sourcesList = []
        if type(source.workon) == StringType or \
           type(source.workon) == UnicodeType:
            sourcesList.append(source.getLynk())
        else:
            allSources = source.workon
            for onesource in allSources:
                source.workon = onesource
                sourcesList.append(source.getLynk())
            # put back to how it was
            source.workon = allSources

        # destinations list
        destsList   = []
        if type(dest.workon) == StringType or \
           type(dest.workon) == UnicodeType:
            destsList = [dest.getLynk()]*len(sourcesList)
        else:
            allDests = dest.workon
            for onedest in allDests:
                dest.workon = onedest
                destsList.append(dest.getLynk())
            # put back to how it was
            dest.workon = allDests

        setProxy = ''  
        if proxy is not None:
            self.checkUserProxy(proxy)
            setProxy =  "export X509_USER_PROXY=" + str(proxy) + " && "

        fname = None
        exitcode, outputs = "", ""
        # create the list of source, destination pairs and write to the temp file
        toCopy = "\n".join([t[0] + " " + t[1] for t in map(None, sourcesList, destsList)]) + "\n"
        try:
            # make a temporary file to contain the list of source, destination pairs
            tmp, fname = tempfile.mkstemp( "", "seapi_", os.getcwd() )
            os.close( tmp )
            file(fname, 'w').write( toCopy )
            # construct the copy command with the tempfile as the argument
            cmd = setProxy + " globus-url-copy -vb -cd -f " + fname
            # do the copy and log the output
            exitcode, outputs = self.executeCommand(cmd)
        finally:
            # remove the temp file
            os.unlink( fname )
 
        ### simple output parsing ###
        #problems = self.simpleOutputCheck(outputs)
        resvalList = self.lessSimpleOutputCheck(outputs, sourcesList)

        #if exitcode != 0 or len(problems) > 0:
        #if exitcode != 0 or oneOrMoreProblems:
            #raise TransferException("Error copying " + toCopy, resvalList, outputs )

        return resvalList

    def move(self, source, dest, proxy = None, opt = ""):
        """
        copy() + delete()
        """
        self.copy([source], [dest], proxy, opt)
        self.delete(source, proxy, opt)

    def delete(self, source, proxy = None, opt = ""):
        """
        Uberftp -> rm
        """
        import os

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"rm %s\" " % (source.hostname, os.path.join("/", source.workon))

        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if ' Is a directory' in problems:
            self.deleteDir( source, proxy, opt )
        elif exitcode != 0 or len(problems) > 0:
            raise OperationException("Error deleting [" +source.workon+ "]", \
                                      problems, outputs )

    def checkExists(self, source, proxy = None, opt = ""):
        """
        Uberftp -> ls
        """

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" " % \
              (source.hostname, os.path.join("/", source.workon))
        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)

        if exitcode != 0 or len(problems) > 0:
            return False
        return True

    def getFileSize(self, source, proxy = None, opt = ""):
        """
        Uberftp -> ls
        """
        import os

        precmd = ""

        opt += " --verbose "
        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" | awk '{print $5}' " % (source.hostname, os.path.join("/", source.workon))

        exitcode, outputs = self.executeCommand(cmd)

        ### simple output parsing ###
        problems = self.simpleOutputCheck(outputs)
        if exitcode != 0 or len(problems) > 0:
            raise OperationException("Error getting size for [" +source.workon+ "]",
                                      problems, outputs )
        return outputs


    def getTurl(self, source, proxy = None, opt = ""):
        """
        return the gsiftp turl
        """
        return source.getLynk()


    def listPath(self, source, proxy = None, opt = ""):
        """
        list of dir [uberftp -> ls]
        """
        import os

        precmd = ""

        if proxy is not None:
            precmd += "env X509_USER_PROXY=%s " % str(proxy)
            self.checkUserProxy(proxy)

        cmd = precmd + "uberftp %s \"ls %s\" " % (source.hostname, os.path.join("/", source.workon))
        exitcode, outputs = self.executeCommand(cmd)

        if exitcode != 0:
            raise OperationException("Error listing [" +source.workon+ "]", \
                                      outputs, outputs )

        filesres = []
        for filet in outputs.split('\n'):
            import os
            if filet != "." and filet != "..":
                filesres.append( os.path.join(source.getFullPath(), filet) )

        return filesres

