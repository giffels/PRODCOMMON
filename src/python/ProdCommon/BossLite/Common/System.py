#!/usr/bin/env python
"""
_System_

"""

from ProdCommon.BossLite.Common.Exceptions import TimeOut
from subprocess import Popen, PIPE, STDOUT
import time
import os
import logging
import select, signal, fcntl

__version__ = "$Id: System.py,v 1.7 2008/10/02 18:07:41 gcodispo Exp $"
__revision__ = "$Revision: 1.7 $"


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
        try:
            os.killpg( os.getpgid(p.pid), signal.SIGTERM)
            os.kill( p.pid, signal.SIGKILL)
            p.wait()
            p.stdout.close()
            del( p )
        except OSError, err :
            logging.warning('an error occurred killing subprocess [%s]' \
                            % str(err) )

        raise TimeOut( command, ''.join(outc), timeout, start, stop )

    try:
        p.wait()
        p.stdout.close()
    except OSError, err:
        logging.warning('an error occurred closing subprocess [%s]' \
                        % str(err) )

    returncode = p.returncode
    if returncode is None :
        returncode = -666666
    del( p )

    return ''.join(outc), returncode



def evalStdList( strList ) :
    """
    _evalStdList_

    eval of a string which is espected to be a list
    it works for strings created with str([...])
    """

    strList = strList[ 1 : -1 ]

    if strList == '':
        return []
    if strList[0] == "'" or strList[0] == '"':
        return [ str(val[ 1 : -1 ]) for val in strList.split(', ') ]
    else :
        return [ str(val) for val in strList.split(',') ]


def evalCustomList( strList ) :
    """
    _evalCustomList_

    eval of a string which is espected to be a list
    it works for any well formed string representing a list
    """
    
    strList = strList[ strList.find('[')+1 : strList.rfind(']') ].strip()

    if strList == '':
        return []
    if strList[0] == "'": 
        return [ str(val[ val.find("'")+1 : val.rfind("'") ])
                 for val in strList.split(',') ]
    elif strList[0] == '"':
        return [ str(val[ val.find('"')+1 : val.rfind('"') ])
                 for val in strList.split(',') ]
    else :
        return [ str(val) for val in strList.split(',') ]
