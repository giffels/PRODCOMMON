#!/usr/bin/env python

"""
_SplitterMaker_

For a given dataset, create and return a JobSplitter instance


"""

import logging


from ProdCommon.DataMgmt.JobSplit.JobSplitter import JobSplitter
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader



def createJobSplitter(dataset, dbsUrl, onlyClosedBlocks = False):
    """
    _createJobSplitter_

    Instantiate a JobSplitter instance for the dataset provided
    and populate it with details from DBS.


    """
    reader = DBSReader(dbsUrl)
    result = JobSplitter(dataset)

    datasetContent = reader.getFiles(dataset, onlyClosedBlocks)

    for blockName, blockData in datasetContent.items():
        locations = blockData['StorageElements']
        newBlock = result.newFileblock(blockName, * locations)
        totalEvents = 0
        fileList = set()
        for fileInfo in blockData['Files']:
            totalEvents += fileInfo['NumberOfEvents']
            fileList.add(fileInfo['LogicalFileName'])
            newBlock.addFile(fileInfo['LogicalFileName'],
                             fileInfo['NumberOfEvents'])
        logging.debug("Block %s contains %s events in %s files" %(
            blockName, totalEvents, len(fileList),
            ))

    return result
    
    
def splitDatasetByEvents(dataset, dbsUrl, eventsPerJob,
                         onlyClosedBlocks = False,
                         siteWhitelist = [], blockWhitelist = []):

    """
    _splitDatasetByEvents_

    API to split a dataset into eventsPerJob sized jobs

    """
    filterSites = len(siteWhitelist) > 0
    filterBlocks = len(blockWhitelist) > 0
    
    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks)
    allJobs = []
    for block in splitter.listFileblocks():
        logging.debug("Processing Block: %s" % block)
        blockInstance = splitter.fileblocks[block]
        if blockInstance.isEmpty():
            msg = "Fileblock is empty: \n%s\n" % block
            msg += "Contains either no files or no SE Names\n"
            logging.warning(msg)
            continue

        if filterSites:
            siteMatches = filter(
                lambda x:x in blockInstance.seNames, siteWhitelist
                )
            if len(siteMatches) == 0:
                msg = "Excluding block %s based on sites: %s \n" % (
                    block, blockInstance.seNames,
                    )
                logging.debug(msg)
                continue
        if filterBlocks:
            if block not in blockWhitelist:
                msg = "Excluding block %s based on block whitelist: %s\n" % (
                    block, blockWhitelist)
                logging.debug(msg)
                continue
            
                
        jobDefs = splitter.splitByEvents(block, eventsPerJob)
        allJobs.extend(jobDefs)
    return allJobs
        

def splitDatasetByFiles(dataset, dbsUrl, filesPerJob,
                        onlyClosedBlocks = False,
                        siteWhitelist = [], blockWhitelist = []):
    """
    _splitDatasetByFiles_

    API to split a dataset into filesPerJob sized jobs

    """
    filterSites = len(siteWhitelist) > 0
    filterBlocks = len(blockWhitelist) > 0

    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks)
    allJobs = []
    
    for block in splitter.listFileblocks():
        blockInstance = splitter.fileblocks[block]
        logging.debug("Processing Block: %s" % block)
        

        if blockInstance.isEmpty():
            msg = "Fileblock is empty: \n%s\n" % block
            msg += "Contains either no files or no SE Names\n"
            logging.warning(msg)
            continue

        if filterSites:
            siteMatches = filter(
                lambda x:x in blockInstance.seNames, siteWhitelist
                )
            if len(siteMatches) == 0:
                msg = "Excluding block %s based on sites: %s \n" % (
                    block, blockInstance.seNames,
                    )
                logging.debug(msg)
                continue
        if filterBlocks:
            if block not in blockWhitelist:
                msg = "Excluding block %s based on block whitelist: %s\n" % (
                    block, blockWhitelist)
                logging.debug(msg)
                continue
            

        
            
        jobDefs = splitter.splitByFiles(block, filesPerJob)
        allJobs.extend(jobDefs)
    return allJobs

