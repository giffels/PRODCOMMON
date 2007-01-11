#!/usr/bin/env python
"""
_DBSWriterObjects_

Functions to instantiate and return DBS Objects and insert them
into DBS if required

"""

from dbsApi import DbsApi
from dbsException import *
from dbsApiException import *
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsAlgorithm import DbsAlgorithm
from dbsQueryableParameterSet import DbsQueryableParameterSet
from dbsProcessedDataset import DbsProcessedDataset
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock





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


def createAlgorithm(datasetInfo, configMetadata, apiRef = None):
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
        
    
    psetContent = datasetInfo['PSetContent']
    
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

    
def createProcessedDataset(primaryDataset, algorithm, datasetInfo,
                           apiRef = None):
    """
    _createProcessedDataset_
    

    """
    
    physicsGroup = datasetInfo.get("PhysicsGroup", "NoGroup")
    status = datasetInfo.get("Status", "VALID")
    dataTier = datasetInfo['DataTier']
    name = datasetInfo['ProcessedDataset']
    
    processedDataset = DbsProcessedDataset (
        PrimaryDataset = primaryDataset,
        AlgoList=[algorithm],
        Name = name,
        TierList = [dataTier],
        PhysicsGroup = physicsGroup,
        Status = status,
        )

    if apiRef != None:
        apiRef.insertProcessedDataset(processedDataset)
        
    return processedDataset

def createDBSFiles(fjrFileInfo):
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
        algo = createAlgorithmForInsert(dataset)
        processed = createProcessedDataset(primary, algo, dataset)

        dbsFileInstance = DbsFile(
            Checksum = checksum,
            NumberOfEvents = nEvents, 
            LogicalFileName = fjrFileInfo['LFN'],
            FileSize = int(fjrFileInfo['Size']),
            Status = "VALID",
            ValidationStatus = 'VALID',
            FileType = 'EVD',
            Dataset = processed,
            TierList = [dataset['DataTier']],
            AlgoList = [algo],
            ParentList = inputLFNs
            )
        
        results.append(dbsFileInstance)
    return results

def createDBSFileBlock(procDataset, apiRef = None):
    """
    _createDBSFileBlock_

    Create a DBS FileBlock instance

    """
    
    
