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
        for lfn, events in blockData['Files'].items():
            newBlock.addFile(lfn, events)

    return result
    
    
def splitDatasetByEvents(dataset, dbsUrl, eventsPerJob,
                         onlyClosedBlocks = False):

    """
    _splitDatasetByEvents_

    API to split a dataset into eventsPerJob sized jobs

    """
    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks)
    allJobs = []
    for block in splitter.listFileblocks():
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
                        onlyClosedBlocks = False):
    """
    _splitDatasetByFiles_

    API to split a dataset into filesPerJob sized jobs

    """
    splitter = createJobSplitter(dataset, dbsUrl, onlyClosedBlocks)
    allJobs = []
    
    for block in splitter.listFileblocks():
        blockInstance = splitter.fileblocks[block]
        if blockInstance.isEmpty():
            msg = "Fileblock is empty: \n%s\n" % block
            msg += "Contains either no files or no SE Names\n"
            logging.warning(msg)
            continue
        jobDefs = splitter.splitByFiles(block, filesPerJob)
        allJobs.extend(jobDefs)
    return allJobs

