#!/usr/bin/env python
"""
_TMDBInject_


Command wrapper for calling TMDBInject


"""

import logging
import os, fcntl, select, sys, string
from subprocess import Popen, PIPE
from ProdCommon.DataMgmt.PhEDEx.DropMaker import makePhEDExDrop
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DataMgmtError import DataMgmtError

class TMDBInjectError(DataMgmtError):
    """
    _TMDBInjectError_
                                                                                                                                                 
    Generic Exception for TMDBInject Error
                                                                                                                                                 
    """
    def __init__(self, msg, **data):
        DataMgmtError.__init__(self, msg, 1003, **data)

def makeNonBlocking(fd):
    """
    _makeNonBlocking_
 
    Make the file descriptor provided non-blocking
 
    """
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)


def runCommand(command):
    """
    _runCommand_
                                                                                                                                                 
    Run the command without deadlocking stdout and stderr
    Return the output + error and exitcode
                                                                                                                                                 
    """
    child = Popen(command, shell = True, stdout = PIPE, stderr = PIPE)
    outfile = child.stdout
    outfd = outfile.fileno()
    errfile = child.stderr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)            # don't deadlock!
    makeNonBlocking(errfd)
    #outdata = errdata = ''
    outdata = []
    errdata = []
    outeof = erreof = 0
    while 1:
        ready = select.select([outfd,errfd],[],[]) # wait for input
        if outfd in ready[0]:
            try:
                outchunk = outfile.read()
            except Exception, ex:
                msg = "Unable to read stdout chunk... skipping"
                logging.debug(msg)
                outchunk = ''
            if outchunk == '': outeof = 1
            #sys.stdout.write(outchunk)
            outdata.append(outchunk)
        if errfd in ready[0]:
            try:
                errchunk = errfile.read()
            except Exception, ex:
                msg = "Unable to read stderr chunk... skipping \n %s"%str(ex)
                logging.debug(msg)
                errchunk = ""
            if errchunk == '': erreof = 1
            #sys.stderr.write(errchunk)
            errdata.append(errchunk)
        if outeof and erreof: break
        select.select([],[],[],.1) # give a little time for buffers to fill
                                                                                                                                                 
    excode = child.wait()
    output = string.join(outdata,"")
    error = string.join(errdata,"")
    output = output + error

    return output,excode

def removedropXML(xmlFile):
    """
      remove the dropXML file
    """
    try:
      os.remove(xmlFile)
    except:
      logging.error("error removing XML drop file %s"%xmlFile)
      pass
                                                                                                                                          
    return

def tmdbInject(phedexConfig, xmlFile, nodes, *storageElements):
    """
    _tmdbInject_


    Invoked TMDBInject with the phedexConfiguration provided to
    inject the XML drop file for the list of storage elements provided

    """

    command = "TMDBInject -version0 -db %s " % phedexConfig

    if nodes:
      command +=" -nodes=%s " % nodes 
    else:
      seList = ""
      for se in storageElements:
        seList += "%s," % se
      seList = seList[:-1]
      command +=" -storage-elements=%s " % seList


    command += " -filedata %s" % xmlFile

    logging.info("Calling: %s" % command)

    #  //
    # // TODO: Run the command, check for errors etc
    #//
    try:
        StdOutput,exitCode = runCommand(command)
        msg = "Command :\n%s\n exited with status: %s" % (command, exitCode)
        logging.info(msg)
        logging.debug("Command StdOut/Err: \n %s"%StdOutput)
    except Exception, ex:
        msg = "Exception while invoking command:\n"
        msg += "%s\n" % command
        msg += "Exception: %s\n" % str(ex)
        removedropXML(xmlFile)
        raise TMDBInjectError(msg)
    if exitCode:
        msg = "Command :\n%s\n  exited non-zero"% command
        msg += "Command StdOut/Err: \n %s"%StdOutput
        removedropXML(xmlFile)
        raise TMDBInjectError(msg)

    removedropXML(xmlFile)
    return


def tmdbInjectBlock(dbsUrl, datasetPath, blockName, phedexConfig,
                    workingDir="/tmp", nodes=None, storageElements=None):
    """
    _tmdbInjectBlock_

    Util Method for injecting a fileblock into TMDB

    

    """

    fileName = blockName.replace("/","_")
    fileName = fileName.replace("#","")
    dropXML = "%s/%s-PhEDExDrop.xml" % (workingDir, fileName)
    
    xmlContent = makePhEDExDrop(dbsUrl, datasetPath, blockName)
    handle = open(dropXML, 'w')
    handle.write(xmlContent)
    handle.close()

    reader = DBSReader(dbsUrl)
    
    if not storageElements:
        storageElements = reader.listFileBlockLocation(blockName)
    
    tmdbInject(phedexConfig, dropXML, nodes, *storageElements )

    return

