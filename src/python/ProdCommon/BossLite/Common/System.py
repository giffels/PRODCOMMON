#!/usr/bin/env python
"""
_System_

"""

from ProdCommon.BossLite.Common.Exceptions import TimeOut
from subprocess import Popen, PIPE, STDOUT
import time
import os
import select, signal, fcntl
from os import popen4

__version__ = "$Id: $"
__revision__ = "$Revision: $"


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
    fcntl.fcntl(fd, fcntl.F_SETFL, flags| os.O_NONBLOCK)

    # return values
    timedOut = False
    outc = []
    while 1:
        (r,w,e) = select.select([fd], [], [], timeout)
        if r == [] and w == [] and e == [] :
            timedOut = True
            break

        read = p.stdout.read()
        if read != '' :
            outc.append( read )
        else :
            break

    if timedOut :
        os.killpg( os.getpgid(p.pid), signal.SIGTERM)
        os.kill( p.pid, signal.SIGKILL)
        p.wait()
        p.stdout.close()
        del( p )
        raise TimeOut( timeout, start, time.time() )
    
    return ''.join(outc)


    # // first possibility
    # p = Popen( command, shell=True,
    #            stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True )

    # os.setpgid( p.pid, 0 )

    # while p.poll() is None and time.time() < finish :
    #     pass #FIXME more CPU friendly

    # result = self.p.poll();
    # if result is None:
    #     os.killpg( os.getpgid(p.pid), signal.SIGKILL)

    # msg = p.stdout.read()


    # // second possibility
    # p = Popen( command, shell=True,
    #            stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True )

    # os.setpgid( p.pid, 0 )

    # timedOut = True
    # while time.time() < finish:
    #     (r,w,e) = select.select([p.pid],[],[],self.timeout)
    #     if len(r) >= 0:
    #         timedOut = False
    #         break
    # 
    # if timedOut :
    #     os.killpg( os.getpgid(p.pid), signal.SIGKILL)

    # msg = p.stdout.read()

    # // third possibility
    # pin, pout = popen4( command )
    # msg = pout.read()
    # return msg



