#!/usr/bin/env python
"""
_System_

"""

from ProdCommon.BossLite.Common.Exceptions import TimeOut
from subprocess import Popen, PIPE, STDOUT
import time
import os
import select, signal, fcntl

__version__ = "$Id: System.py,v 1.3 2008/06/30 15:41:04 gcodispo Exp $"
__revision__ = "$Revision: 1.3 $"


def setPgid():
    """
    preexec_fn for Popen to set subprocess pgid
    
    """

    os.setpgid( os.getpid(), 0 )


def executeCommand( command, timeout=None ):
    """
    _executeCommand_

    Util it execute the command provided in a popen object with a timeout
    """

    start = time.time()
    p = Popen( command, shell=True, \
               stdin=PIPE, stdout=PIPE, stderr=STDOUT, \
               close_fds=True, preexec_fn=setPgid )

    # playing with fd
    fd = p.stdout.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    # return values
    timedOut = False
    outc = []

    while 1:
        (r, w, e) = select.select([fd], [], [], timeout)

        if fd not in r :
            timedOut = True
            break

        read = p.stdout.read()
        if read != '' :
            outc.append( read )
        else :
            break

    if timedOut :
        stop = time.time()
        os.killpg( os.getpgid(p.pid), signal.SIGTERM)
        os.kill( p.pid, signal.SIGKILL)
        p.wait()
        p.stdout.close()
        del( p )
        raise TimeOut( command, ''.join(outc), timeout, start, stop )
    
    return ''.join(outc)


