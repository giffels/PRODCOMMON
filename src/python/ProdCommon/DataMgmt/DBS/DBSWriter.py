#!/usr/bin/env python
"""
_DBSWriter_

Interface object for writing data to DBS

"""



from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *


import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetsWithPSet
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasets
from ProdCommon.MCPayloads.MergeTools import createMergeDatasetWorkflow





class _CreateDatasetOperator:
    """
    _CreateDatasetOperator_

    Operator for creating datasets from a workflow node

    """
    def __init__(self, apiRef, workflow):
        self.apiRef = apiRef
        self.workflow = workflow
        
    def __call__(self, pnode):
        if pnode.type != "CMSSW":
            return
        datasets = getOutputDatasetsWithPSet(pnode)
        cfgMeta = None
        try:
            cfgInt = CfgInterface(pnode.configuration, True)
            cfgMeta = cfgInt.configMetadata()
            cfgMeta['Type'] = self.workflow.parameters["RequestCategory"]      
        except Exception, ex:
            msg = "Unable to Extract cfg data from workflow"
            msg += str(ex)
            print msg
            return
            
        for dataset in datasets:
            primary = DBSWriterObjects.createPrimaryDataset(
                dataset, self.apiRef)
            algo = DBSWriterObjects.createAlgorithm(
                dataset, cfgMeta, self.apiRef)
            
            processed = DBSWriterObjects.createProcessedDataset(
                primary, algo, dataset, self.apiRef)
            
        return

class _CreateMergeDatasetOperator:
    """
    _CreateMergeDatasetOperator_

    Operator for creating merge datasets from a workflow node

    """
    def __init__(self, apiRef, workflow):
        self.apiRef = apiRef
        self.workflow = workflow
    
    def __call__(self, pnode):
        if pnode.type != "CMSSW":
            return
        for dataset in pnode._OutputDatasets:
            mergeAlgo = DBSWriterObjects.createMergeAlgorithm(dataset,
                                                              self.apiRef)
            inputDataset = dataset.get('ParentDataset', None)
            if inputDataset == None:
                continue
            processedDataset = dataset["ProcessedDataset"]
            self.apiRef.insertMergedDataset(
                inputDataset, processedDataset, mergeAlgo)
        return
    
            
        
#  //
# // Util lambda for matching files with the same dataset and se name
#//
fileMatcher = lambda x, dataset, seName: (x['CompleteDatasetName'] == dataset) and (x['SEName'] == seName)
makeDSName = lambda x: "/%s/%s/%s" % (x['PrimaryDataset'],
                                      x['DataTier'],
                                      x['ProcessedDataset'])
makeDBSDSName = lambda x: "/%s/%s/%s" % (
    x['Dataset']['PrimaryDataset']['Name'],
    x['Dataset']['TierList'][0],
    x['Dataset']['Name'])


class _InsertFileList(list):
    def __init__(self, seName, dataset):
        list.__init__(self)
        self.seName = seName
        self.dataset = dataset

class DBSWriter:
    """
    _DBSWriter_

    General API for writing data to DBS


    """
    def __init__(self, url,  **contact):
        args = { "url" : url}
        args.update(contact)
        self.dbs = DbsApi(args)
        self.reader = DBSReader(**args)
        
    def createDatasets(self, workflowSpec):
        """
        _createDatasets_

        Create All the output datasets found in the workflow spec instance
        provided

        """
        try:
            workflowSpec.payload.operate(
                _CreateDatasetOperator(self.dbs, workflowSpec)
                )
        except DbsException, ex:
            msg = "Error in DBSWriter.createDatasets\n"
            msg += "For Workflow: %s\n" % workflowSpec.workflowName()
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        return
        

    def createMergeDatasets(self, workflowSpec, fastMerge = True):
        """
        _createMergeDatasets_

        Create merge output datasets for a workflow Spec
        Expects a Processing workflow spec from which it will generate the
        merge datasets automatically.

        
        """
        mergeSpec = createMergeDatasetWorkflow(workflowSpec, fastMerge)        
        try:
            mergeSpec.payload.operate(
                _CreateMergeDatasetOperator(self.dbs, workflowSpec)
                )
            
        except DbsException, ex:
            msg = "Error in DBSWriter.createMergeDatasets\n"
            msg += "For Workflow: %s\n" % workflowSpec.workflowName()
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        
        return
        

        


    def insertFiles(self, fwkJobRep):
        """
        _insertFiles_

        Process the files in the FwkJobReport instance and insert
        them into the associated datasets

        """

        insertLists = {}
        for outFile in fwkJobRep.files:
            #  //
            # // Convert each file into a DBS File object
            #//
            seName = outFile['SEName']

            try:
                dbsFiles = DBSWriterObjects.createDBSFiles(outFile,
                                                           fwkJobRep.jobType)
            except DbsException, ex:
                msg = "Error in DBSWriter.insertFiles:\n"
                msg += "Error creating DbsFile instances for file:\n"
                msg += "%s\n" % outFile['LFN']
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            
            for f in dbsFiles:
                datasetName = makeDBSDSName(f)
                hashName = "%s-%s" % (seName, datasetName)
                
                if not insertLists.has_key(hashName):
                    insertLists[hashName] = _InsertFileList(seName,
                                                            datasetName)
                insertLists[hashName].append(f)
                
                
            

        #  //Processing Jobs: 
        # // Insert the lists of sorted files into the appropriate
        #//  fileblocks

        for fileList in insertLists.values():
            procDataset = fileList[0]['Dataset']

            try:
                fileBlock = DBSWriterObjects.getDBSFileBlock(
                    self.dbs,
                    procDataset,
                    fileList.seName)
            except DbsException, ex:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "Cannot retrieve FileBlock for dataset:\n"
                msg += " %s\n" % procDataset
                msg += "In Storage Element:\n %s\n" % fileList.seName
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            if fwkJobRep.jobType == "Merge":
                #  //
                # // Merge files
                #//
                for mergedFile in fileList:
                    mergedFile['Block'] = fileBlock
                    try:
                        self.dbs.insertMergedFile(mergedFile['ParentList'],
                                                  mergedFile)
                    except DbsException, ex:
                        msg = "Error in DBSWriter.insertFiles\n"
                        msg += "Cannot insert merged file:\n"
                        msg += "  %s\n" % mergedFile['LogicalFileName']
                        msg += "%s\n" % formatEx(ex)
                        raise DBSWriterError(msg)
            else:
                #  //
                # // Processing files
                #//
                try:
                    self.dbs.insertFiles(procDataset, list(fileList),
                                         fileBlock)
                except DbsException, ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    msg += " %s\n" % (
                        [ x['LogicalFileName'] for x in fileList ],
                        )
                    
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)
        return



    def manageFileBlock(self, fileblockName, maxFiles = 100, maxSize = None):
        """
        _manageFileBlock_

        Check to see wether the fileblock with the provided name
        is closeable based on number of files or total size.

        If the block equals or exceeds wither the maxFiles or maxSize
        parameters, close the block and return True, else do nothing and
        return False

        """
        #  //
        # // Check that the block exists, and is open before we close it
        #//
        blockInstance = self.dbs.listBlocks(block_name=fileblockName)
        if len(blockInstance) > 1:
            msg = "Multiple Blocks matching name: %s\n" % fileblockName
            msg += "Unable to manage file block..."
            raise DBSWriterError(msg)

        if len(blockInstance) == 0:
            msg = "Block name %s not found\n" % fileblockName
            msg += "Cant manage a non-existent fileblock"
            raise DBSWriterError(msg)
        blockInstance = blockInstance[0]
        isClosed = blockInstance.get('OpenForWriting', '1')
        if isClosed == "0":
            msg = "Block %s already closed" % fileblockName
            logging.warning(msg)
            return False

        
        
        #  //
        # // We have an open block, sum number of files and file sizes
        #//
        
        fileCount = blockInstance.get('NumberOfFiles', 0)
        totalSize = blockInstance.get('BlockSize', 0)
        
        msg = "Fileblock: %s\n ==> Size: %s Files: %s\n" % (
            fileblockName, totalSize, fileCount)
        logging.warning(msg)

        #  //
        # // Test close block conditions
        #//
        closeBlock = False
        if fileCount >= maxFiles:
            closeBlock = True
            msg = "Closing Block Based on files: %s" % fileblockName
            logging.debug(msg)
            
        if maxSize != None:
            if totalSize >= maxSize:
                closeBlock = True
                msg = "Closing Block Based on size: %s" % fileblockName
                logging.debug(msg)
                

        if closeBlock:
            #  //
            # // Close the block
            #//
            self.dbs.closeBlock(
                DBSWriterObjects.createDBSFileBlock(fileblockName)
                )
        return closeBlock
    
        
