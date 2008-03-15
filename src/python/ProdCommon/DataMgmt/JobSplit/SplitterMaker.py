#!/usr/bin/env python

"""
_SplitterMaker_

For a given dataset, create and return a JobSplitter instance


"""

import logging


from ProdCommon.DataMgmt.JobSplit.JobSplitter import JobSplitter
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader



def createJobSplitter(dataset, dbsUrl, onlyClosedBlocks = False,
                      siteWhitelist = [], blockWhitelist = [],
                      withParents = False):
    """
    _createJobSplitter_

    Instantiate a JobSplitter instance for the dataset provided
    and populate it with details from DBS.


    """
    reader = DBSReader(dbsUrl)
    result = JobSplitter(dataset)
    filterSites = len(siteWhitelist) > 0
    filterBlocks = len(blockWhitelist) > 0
    
    for blockName in reader.listFileBlocks(dataset, onlyClosedBlocks):
        locations = reader.listFileBlockLocation(blockName)
        if filterBlocks:
            if blockName not in blockWhitelist:
                msg = "Excluding block %s based on block whitelist: %s\n" % (
                    blockName, blockWhitelist)
                logging.debug(msg)
                continue

        if filterSites:
            siteMatches = filter(
                lambda x:x in locations, siteWhitelist
                )

            if len(siteMatches) == 0:
                msg = "Excluding block %s based on sites: %s \n" % (
                    blockName, locations,
                    )
                logging.debug(msg)
                continue
            else:
                locations = siteMatches


        newBlock = result.newFileblock(blockName, * locations)


        if withParents == True:
            blockData = reader.getFileBlockWithParents(blockName)[blockName]
        else:
            blockData = reader.getFileBlock(blockName)[blockName]

        
        totalEvents = 0
        fileList = set()
        for fileInfo in blockData['Files']:
            totalEvents += fileInfo['NumberOfEvents']
            fileList.add(fileInfo['LogicalFileName'])
            if withParents:
                parList = [ x['LogicalFileName']
                            for x in fileInfo['ParentList'] ]
            
                newBlock.addFile(fileInfo['LogicalFileName'],
                                 fileInfo['NumberOfEvents'],
                                 parList)
            else:
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
    
    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks, siteWhitelist, blockWhitelist)
    allJobs = []
    for block in splitter.listFileblocks():
        logging.debug("Processing Block: %s" % block)
        blockInstance = splitter.fileblocks[block]
        if blockInstance.isEmpty():
            msg = "Fileblock is empty: \n%s\n" % block
            msg += "Contains either no files or no SE Names\n"
            logging.warning(msg)
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

    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks,
                                 siteWhitelist, blockWhitelist)
    allJobs = []
    
    for block in splitter.listFileblocks():
        blockInstance = splitter.fileblocks[block]
        logging.debug("Processing Block: %s" % block)
        

        if blockInstance.isEmpty():
            msg = "Fileblock is empty: \n%s\n" % block
            msg += "Contains either no files or no SE Names\n"
            logging.warning(msg)
            continue

        
        
            
        jobDefs = splitter.splitByFiles(block, filesPerJob)
        allJobs.extend(jobDefs)
    return allJobs


def splitDatasetForReReco(dataset, dbsUrl,
                          onlyClosedBlocks = False,
                          siteWhitelist = [], blockWhitelist = []):
    """
    _splitDatasetByFiles_

    API to split a dataset into filesPerJob sized jobs

    """
    filterSites = len(siteWhitelist) > 0
    filterBlocks = len(blockWhitelist) > 0

    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks,
                                 siteWhitelist, blockWhitelist, True)
    allJobs = []
    
    for block in splitter.listFileblocks():
        blockInstance = splitter.fileblocks[block]
        logging.debug("Processing Block: %s" % block)
        

        if blockInstance.isEmpty():
            msg = "Fileblock is empty: \n%s\n" % block
            msg += "Contains either no files or no SE Names\n"
            logging.warning(msg)
            continue

        
        
            
        jobDefs = splitter.splitByFiles(block, 1)
        allJobs.extend(jobDefs)
    return allJobs

