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
        args = { "url" : url}
        args.update(contact)
        self.dbs = DbsApi(args)


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


    def listFileBlocks(self, dataset, onlyClosedBlocks = False):
        """
        _listFileBlocks_

        Retrieve a list of fileblock names for a dataset

        """

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


    def listFilesInBlock(self, fileBlockName):
        """
        _listFilesInBlock_

        Get a list of files in the named fileblock

        """
        try:
            files = self.dbs.listFiles("", fileBlockName)
        except DbsException, ex:
            msg = "Error in "
            msg += "DBSReader.listFilesInBlock(%s)\n" % fileBlockName
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)

        result = {}
        [ result.__setitem__(x['LogicalFileName'], x['NumberOfEvents']) \
            for x in files ]
        return result

        

    def listFileBlockLocation(self, fileBlockName):
        """
        _listFileBlockLocation_

        Get a list of fileblock locations

        """
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

          
        
