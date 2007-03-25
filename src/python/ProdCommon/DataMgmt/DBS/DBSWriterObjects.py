#!/usr/bin/env python
"""
_DBSWriterObjects_

Functions to instantiate and return DBS Objects and insert them
into DBS if required

"""

import logging

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsAlgorithm import DbsAlgorithm
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsStorageElement import DbsStorageElement


def makeTierList(dataTier):
    """
    _makeTierList_
 
    Standard tool to split data tiers if they contain - chars
    *** Do not use outside of this module ***
 
    """
    tierList = dataTier.split("-")
    return tierList


def createPrimaryDataset(datasetInfo, apiRef = None):
    """
    _createPrimaryDataset_

    Create and return a Primary Dataset object. 
    If apiRef is not None, it is used to insert the dataset into the
    DBS

    """
    primary = DbsPrimaryDataset(Name = datasetInfo["PrimaryDataset"])
    if apiRef != None:
        apiRef.insertPrimaryDataset(primary)
    return primary


def createAlgorithm(datasetInfo, configMetadata = None, apiRef = None):
    """
    _createAlgorithm_
    
    Create an algorithm assuming that datasetInfo is a
    ProdCommon.MCPayloads.DatasetInfo like dictionary

    """
    exeName = datasetInfo['ApplicationName']
    appVersion = datasetInfo['ApplicationVersion']
    appFamily = datasetInfo["ApplicationFamily"]
    psetHash = datasetInfo['PSetHash']
    if psetHash.find(";"):
        # no need for fake hash in new schema
        psetHash = psetHash.split(";")[0]
        psetHash = psetHash.replace("hash=", "")
        
    #
    # HACK:  Problem with large PSets
    #
    psetContent = datasetInfo['PSetContent']
    msg = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
    msg += "TEST HACK USED FOR PSetContent\n" 
    msg += ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    logging.warning(msg)
    print msg
    psetContent = "This is not a PSet"
    
    #
    # HACK: 100 char limit on cfg file name
    if configMetadata != None:
        cfgName = configMetadata['Name']
        if len(cfgName) > 100:
            msg = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
            msg += "TEST HACK USED FOR Config File Name"
            msg += ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
            logging.warning(msg)
            print msg
            configMetadata['Name'] = cfgName[-99]
    
        psetInstance = DbsQueryableParameterSet(
            Hash = psetHash,
            Name = configMetadata['Name'],
            Version = configMetadata['Version'],
            Type = configMetadata['Type'],
            Annotation = configMetadata['Annotation'],
            Content = psetContent, 
            )

        
        algorithmInstance = DbsAlgorithm(
            ExecutableName = exeName,
            ApplicationVersion = appVersion,
            ApplicationFamily = appFamily,
            ParameterSetID = psetInstance
            )
    else:
        algorithmInstance = DbsAlgorithm(
            ExecutableName = exeName,
            ApplicationVersion = appVersion,
            ApplicationFamily = appFamily,
            )
        
        
    if apiRef != None:
        apiRef.insertAlgorithm(algorithmInstance)
    return algorithmInstance

def createAlgorithmForInsert(datasetInfo):
    """
    _createPartialAlgorithm_

    Create an Algorithm instance that uses the minimal info needed
    to insert a file

    """
    exeName = datasetInfo['ApplicationName']
    appVersion = datasetInfo['ApplicationVersion']
    appFamily = datasetInfo["ApplicationFamily"]
    psetHash = datasetInfo['PSetHash']
    if psetHash.find(";"):
        # no need for fake hash in new schema
        psetHash = psetHash.split(";")[0]
        psetHash = psetHash.replace("hash=", "")
    
    psetInstance = DbsQueryableParameterSet(
        Hash = psetHash)
    algorithmInstance = DbsAlgorithm(
        ExecutableName = exeName,
        ApplicationVersion = appVersion,
        ApplicationFamily = appFamily,
        ParameterSetID = psetInstance
        )
    return algorithmInstance

def createMergeAlgorithm(datasetInfo, apiRef = None):
    """
    _createMergeAlgorithm_

    Create a DbsAlgorithm for a merge dataset

    """
    exeName = datasetInfo['ApplicationName']
    version = datasetInfo['ApplicationVersion']
    family = datasetInfo.get('ApplicationFamily', None)
    if family == None:
        family = datasetInfo['OutputModuleName']

    
    mergeAlgo = DbsAlgorithm (
        ExecutableName = exeName,
        ApplicationVersion = version,
        ApplicationFamily = family,
        )

    if apiRef != None:
        apiRef.insertAlgorithm(mergeAlgo)
    return mergeAlgo


    
    
def createProcessedDataset(primaryDataset, algorithm, datasetInfo,
                           apiRef = None):
    """
    _createProcessedDataset_
    

    """
    
    physicsGroup = datasetInfo.get("PhysicsGroup", "NoGroup")
    status = datasetInfo.get("Status", "VALID")
    dataTier = datasetInfo['DataTier']

    tierList = makeTierList(datasetInfo['DataTier'])

    name = datasetInfo['ProcessedDataset']
   
    processedDataset = DbsProcessedDataset (
        PrimaryDataset = primaryDataset,
        AlgoList=[algorithm],
        Name = name,
        TierList = tierList,
        PhysicsGroup = physicsGroup,
        Status = status,
        )

    if apiRef != None:
        apiRef.insertProcessedDataset(processedDataset)
        
    return processedDataset

def createDBSFiles(fjrFileInfo, jobType = None):
    """
    _createDBSFiles_

    Create a list of DBS File instances from the file details contained
    in a FwkJobRep.FileInfo instance describing an output file
    Does not insert files, returns as list of DbsFile objects
    
    """
    results = []
    inputLFNs = [ x['LFN'] for x in fjrFileInfo.inputFiles]
    checksum = fjrFileInfo.checksums['cksum']
    nEvents = int(fjrFileInfo['TotalEvents'])
    
    for dataset in fjrFileInfo.dataset:
        primary = createPrimaryDataset(dataset)
        if jobType == "Merge":
            algo = createMergeAlgorithm(dataset)
        else:
            algo = createAlgorithmForInsert(dataset)
        processed = createProcessedDataset(primary, algo, dataset)


        dbsFileInstance = DbsFile(
            Checksum = checksum,
            NumberOfEvents = nEvents, 
            LogicalFileName = fjrFileInfo['LFN'],
            FileSize = int(fjrFileInfo['Size']),
            Status = "VALID",
            ValidationStatus = 'VALID',
            FileType = 'EDM',
            Dataset = processed,
            TierList = makeTierList(dataset['DataTier']),
            AlgoList = [algo],
            ParentList = inputLFNs,
            BranchList = fjrFileInfo.branches,
            )
        
        results.append(dbsFileInstance)
    return results


def createDBSStorageElement(seName):
    """
    _createDBSStorageElement_

    """
    return DbsStorageElement(Name = seName)

    
def getDBSFileBlock(dbsApiRef, procDataset, seName):
    """
    _getDBSFileBlock_

    Given the procDataset and seName provided, get the currently open
    file block for that dataset/se pair.
    If an open block does not exist, then create a new block and
    return that

    """
    allBlocks = dbsApiRef.listBlocks(procDataset, block_name = "*",
                                     storage_element_name = seName)
    
        
    openBlocks = [b for b in allBlocks if str(b['OpenForWriting']) == "1"]


    blockRef = None
    if len(openBlocks) > 1:
        msg = "Too many open blocks for dataset:\n"
        msg += "SE: %s\n" % seName
        msg += "Dataset: %s\n" %procDataset
        msg += "Using last open block\n"
        logging.warning(msg)
        blockRef = openBlocks[-1]
    elif len(openBlocks) == 1:
        blockRef = openBlocks[0]

    if blockRef == None:
        #  //
        # // Need to create new block
        #//
        
        
        newBlockName = dbsApiRef.insertBlock (procDataset, None ,
                                              storage_element_list = [seName])

        # get from DBS listBlocks API the DbsFileBlock newly inserted   
        blocks=dbsApiRef.listBlocks(procDataset, block_name = newBlockName )
        if len(blocks) > 1:
          msg = "Too many blocks with the same name: %s:\n"%newBlockName
          msg += "Using last block\n"
          logging.warning(msg)
          blockRef = blocks[-1]
        elif len(blocks) == 1:
          blockRef = blocks[0]
        else: 
          msg = "No FileBlock found to add files to"
          logging.error(msg)
          # FIXME: throw an error ?

## StorageElementList below is wrong: it should be a list of dictionary [ { 'Name': seName } ] 
## In order to define the DbsFileBlock it should be enough to specify its blockname and 
## it shouldn't be needed to specify the SE and Dataset again,
## however since this is not the case, it's safer to get the DbsFileBlock from listBlocks DBS API
## rather then defining a DbsFileBlock.
#        blockRef = DbsFileBlock(
#            Name = newBlockName,
#            Dataset = procDataset,
#            StorageElementList = [ seName ] 
#            )


    return blockRef


