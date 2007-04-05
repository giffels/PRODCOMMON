#!/usr/bin/env python
"""
_TMDBInject_


Command wrapper for calling TMDBInject


"""

import logging
from ProdCommon.DataMgmt.PhEDEx.DropMaker import makePhEDExDrop
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

def tmdbInject(phedexConfig, xmlFile, *storageElements):
    """
    _tmdbInject_


    Invoked TMDBInject with the phedexConfiguration provided to
    inject the XML drop file for the list of storage elements provided

    """

    command = "TMDBInject -db %s " % phedexConfig

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
    return
    



def tmdbInjectBlock(dbsUrl, datasetPath, blockName, phedexConfig,
                    workingDir="/tmp"):
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
    storageElements = reader.listFileBlockLocation(blockName)
    
    tmdbInject(phedexConfig, dropXML, *storageElements )

    return
