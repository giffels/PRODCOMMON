#!/usr/bin/env python
"""
_System_

"""

from ProdCommon.BossLite.Common.Exceptions import TimeOut
from subprocess import Popen, PIPE, STDOUT
import time
import os
import select, signal, fcntl

__version__ = "$Id: System.py,v 1.5 2008/09/08 10:20:47 gcodispo Exp $"
__revision__ = "$Revision: 1.5 $"


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

    p.wait()
    p.stdout.close()
    returncode = p.returncode
    if returncode is None :
        returncode = -666666
    del( p )

    return ''.join(outc), returncode



def evalStdList( str ) :
    """
    _evalStd_

    eval of a string which is espected to be a list
    it works for strings created with str([...])
    """

    str = str[ 1 : -1 ]

    if str == '':
        return []
    if str[0] == '"':
        return [ val[ 1 : -1 ] for val in str.split(', ') ]
    elif str[0] == "'":    
        return [ val[ 1 : -1 ] for val in str.split(', ') ]
    else :
        return [ val for val in str.split(',') ]


def evalCustomList( str ) :
    """
    _evalCustom_

    eval of a string which is espected to be a list
    it works for any well formed string representing a list
    """
    
    str = str[ str.find('[')+1 : str.rfind(']') ].strip()

    if str == '':
        return []
    if str[0] == '"':
        return [ val[ val.find('"')+1 : val.rfind('"') ]
                 for val in str.split(',') ]
    elif str[0] == "'":        
        return [ val[ val.find("'")+1 : val.rfind("'") ]
                 for val in str.split(',') ]
    else :
        return [ val for val in str.split(',') ]
