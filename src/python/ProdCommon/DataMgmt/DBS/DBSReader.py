#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx



class DBSReader:
    """
    _DBSReader_

    General API for reading data from DBS


    """
    def __init__(self, url,  **contact):
        args = { "url" : url, "level" : 'ERROR'}
        args.update(contact)
        try:
         self.dbs = DbsApi(args)
        except DbsException, ex:
            msg = "Error in DBSReader with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)


    def listPrimaryDatasets(self, match = None):
        """
        _listPrimaryDatasets_
        
        return a list of primary datasets matching the glob expression.
        If no expression is provided, all datasets are returned
        """
        arg = "*"
        if match != None:
            arg = match
        try:
            result = self.dbs.listPrimaryDatasets(arg)
        except DbsException, ex:
            msg = "Error in DBSReader.listPrimaryDataset(%s)\n" % arg
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
            
        result = [ x['Name'] for x in result ]
        return result

    def matchProcessedDatasets(self, primary, tier, process):
        """
        _matchProcessedDatasets_

        return a list of Processed datasets 
        """
        try:
            result = self.dbs.listProcessedDatasets(primary, tier, process)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        return result


    def listProcessedDatasets(self, primary, dataTier = None):
        """
        _listProcessedDatasets_

        return a list of Processed datasets for the primary and optional
        data tier value

        """
        tier = "*"
        if dataTier != None:
            tier = dataTier

        try:
            result = self.dbs.listProcessedDatasets(primary, tier)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        
        result = [ x['Name'] for x in result ]
        return result
        
    
    def listDatasetFiles(self, datasetPath):
        """
        _listDatasetFiles_

        Get list of files for dataset

        """
        
        print self.dbs.listFiles(datasetPath)

    def getFileBlocksInfo(self, dataset):
        """
        """
        self.checkDatasetPath(dataset)
        try:
             blocks = self.dbs.listBlocks(dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        return blocks

    def listFileBlocks(self, dataset, onlyClosedBlocks = False):
        """
        _listFileBlocks_

        Retrieve a list of fileblock names for a dataset

        """
        self.checkDatasetPath(dataset)
        try:
             blocks = self.dbs.listBlocks(dataset)
        except DbsException, ex:
            msg = "Error in DBSReader.listFileBlocks(%s)\n" % dataset
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        
        if onlyClosedBlocks:
            result = [
                x['Name'] for x in blocks \
                  if str(x['OpenForWriting']) != "1"
                ]

        else:
            result = [ x['Name'] for x in blocks ]
            
        return result

    def blockExists(self, fileBlockName):
        """
        _blockExists_

        Check to see if block with name provided exists in the DBS
        Instance.

        Return True if exists, False if not

        """
        self.checkBlockName(fileBlockName)
        try:

            blocks = self.dbs.listBlocks(block_name = fileBlockName)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.blockExists(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        
        if len(blocks) == 0:
            return False
        return True


    def listFilesInBlock(self, fileBlockName):
        """
        _listFilesInBlock_

        Get a list of files in the named fileblock

        """
        try:
            files = self.dbs.listFiles(
                 "", # path
                 "", #primary
                 "", # processed
                 [], #tier_list
                 "", #analysisDataset
                 fileBlockName)
            
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = []
        [ result.append(dict(x) ) for x in files ]
        return result
    
        

    def listFileBlockLocation(self, fileBlockName):
        """
        _listFileBlockLocation_

        Get a list of fileblock locations

        """
        self.checkBlockName(fileBlockName)
        try:

            blocks = self.dbs.listBlocks(block_name = fileBlockName)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.getFileBlockLocation(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if blocks == []:
            return None

        ses = []
        [ ses.extend(x['StorageElementList']) for x in blocks]

        seList = set()

        [ seList.add(x['Name']) for x in ses ]

        return list(seList)
        
    def getFileBlock(self, fileBlockName):
        """
        _getFileBlock_

        return a dictionary:
        { blockName: {
             "StorageElements" : [<se list>],
             "Files" : { LFN : Events },
             }
        }
        """
        result = { fileBlockName: {
            "StorageElements" : self.listFileBlockLocation(fileBlockName),
            "Files" : self.listFilesInBlock(fileBlockName),
            "IsOpen" : self.blockIsOpen(fileBlockName),
            
            }
                   }
        return result

    

    def getFiles(self, dataset, onlyClosedBlocks = False):
        """
        _getFiles_

        Returns a dictionary of block names for the dataset where
        each block constists of a dictionary containing the StorageElements
        for that block and the files in that block by LFN mapped to NEvents

        """
        result = {}
        blocks = self.listFileBlocks(dataset, onlyClosedBlocks)

        [ result.update(self.getFileBlock(x)) for x in blocks ]

        return result


    def blockIsOpen(self, blockName):
        """
        _blockIsOpen_

        Return True if named block is open, false if not, or if block
        doenst exist

        """
        self.checkBlockName(blockName)
        blockInstance = self.dbs.listBlocks(block_name=blockName)
        if len(blockInstance) == 0:
            return False
        blockInstance = blockInstance[0]
        isOpen = blockInstance.get('OpenForWriting', '1')
        if isOpen == "0":
            return False
        return True

          
        
    def blockToDatasetPath(self, blockName):
        """
        _blockToDatasetPath_

        Given a block name, get the dataset Path associated with that
        Block.

        Returns the dataset path, or None if not found

        """
        self.checkBlockName(blockName)
        try:
            blocks = self.dbs.listBlocks(block_name = blockName)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.blockToDataset(%s)\n" % blockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        if blocks == []:
            return None
        
        pathname = blocks[-1].get('Path', None)
        return pathname
 

    def checkDatasetPath(self,pathName):
        """
         _checkDatasetPath_
        """ 
        if pathName in ("", None):
           raise DBSReaderError( "Invalid Dataset Path name: => %s <=" % pathName)  

    def checkBlockName(self,blockName):
        """
         _checkBlockName_
        """
        if blockName in ("", "*", None):
           raise DBSReaderError( "Invalid Block name: => %s <=" % blockName)

