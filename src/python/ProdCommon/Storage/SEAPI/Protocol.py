from Exceptions import OperationException, SEAPITimeout
import logging, os, time, fcntl, select,signal
from subprocess import Popen, PIPE, STDOUT


class Protocol(object):
    '''Represents any Protocol'''

    def __init__(self, logger = None, tout = None):
        self.logger = logger
        self.timeout= tout
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

        status, output = self.executeCommand( command, timeout = 101 )

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


    def executeCommand(self, command, timeout=None , stderr=False):
        """
        _executeCommand_
 
        Util it execute the command provided in a popen object with a timeout
        """
        if timeout is None and self.timeout is not None:
            timeout = self.timeout
 
        start = time.time()
        p = Popen( command, shell=True, \
                   stdin=PIPE, stdout=PIPE, stderr=PIPE, \
                   close_fds=True, preexec_fn=setPgid )
 
        # playing with fd
        fd = p.stdout.fileno()
        fde = p.stderr.fileno()

        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        flags = fcntl.fcntl(fde, fcntl.F_GETFL)
        fcntl.fcntl(fde, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        # return values
        timedOut = False
        outc = []
        errc = []

        ## We want a timeout based on command execution time
        if timeout is not None:
            temp_tout = 0
            step_tout = float(timeout) / float(100)
            while float(temp_tout) <= float(timeout):
                end = p.poll()
                if end is not None:
                    break;
                else:
                    temp_tout += step_tout
                    time.sleep(step_tout)

            if float(temp_tout) > float(timeout):
                try:
                    os.killpg( os.getpgid(p.pid), signal.SIGTERM)
                    os.kill( p.pid, signal.SIGKILL)
                    p.wait()
                    p.stdout.close()
                    p.stderr.close()
                except OSError, err :
                    pass
                    #logging.warning(
                    #    'Warning: an error occurred killing subprocess [%s]' \
                    #    % str(err) )
                raise SEAPITimeout("Timeout interrupt for too long execution [timeout = %s]."%str(timeout), [], "Stopped execution after %s sec."%str(temp_tout))

        while 1:
            # We want timeout based on command execution time
            # and not on command sleep time
            #(r, w, e) = select.select([fd], [], [], timeout)
            (r, w, e) = select.select([fd, fde], [], [], None)
            read = '' 
            readerr = '' 
            if fd in r:
                read = p.stdout.read()
            if fde in r:
                readerr = p.stderr.read()
            if fd not in r and fde not in r:
                timedOut = True
                break
            if read != '' or readerr != '' :
                if read != '': outc.append( read )
                if readerr != '':errc.append( readerr )
            else :
                break

        if timedOut :
            stop = time.time()
            try:
                os.killpg( os.getpgid(p.pid), signal.SIGTERM)
                os.kill( p.pid, signal.SIGKILL)
                p.wait()
                p.stdout.close()
                p.stderr.close()
            except OSError, err :
                pass 
                #logging.warning(
                #    'Warning: an error occurred killing subprocess [%s]' \
                #    % str(err) )
 
            raise TimeOut( command, ''.join(outc)+ ''.join(errc), timeout, start, stop )
 
        try:
            p.wait()
            p.stdout.close()
            p.stderr.close()
        except OSError, err:
            pass 
            #logging.warning( 'Warning: an error occurred closing subprocess [%s] %s  %s' \
            #                 % (str(err), ''.join(outc)+''.join(errc), p.returncode ))
 
        returncode = p.returncode

        ## we trust None as zero and then leave to the plugin the check on the stdout
        if returncode is None :
            returncode = 0
        
        if stderr == True:
            ## we could remove this.... and only use logging... 
            self.__logout__(str(command), str(returncode), str(''.join(outc)+''.join(errc)))
            return returncode,''.join(outc),''.join(errc)

        #logging.debug(command)
        #logging.debug(returncode)
        #logging.debug(''.join(outc))
        #logging.debug(''.join(errc))
        ## we could remove this.... and only use logging... 
        self.__logout__(str(command), str(returncode), str(''.join(outc)+''.join(errc)))
        return returncode,''.join(outc)+''.join(errc)
        

    #def executeCommand(self, command):
    #    """
    #    common method to execute commands
    #
    #    return exit_code, cmd_out
    #    """
    #    import commands
    #    status, output = commands.getstatusoutput( command )
    #    self.__logout__(str(command), str(status), str(output))
    #    return status, output

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

    def expandEnv(self, env_var, final_path):
        if os.environ.get(env_var):
            return os.path.normpath(os.environ.get(env_var) + str(final_path))
        else:
            return None

def setPgid( ):
    """
    preexec_fn for Popen to set subprocess pgid
    
    """
    os.setpgid( os.getpid(), 0 )
