#!/usr/bin/env python
"""
_DBSWriter_

Interface object for writing data to DBS

"""



from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *


import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects

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
        

    def createDatasets(self, workflowSpec):
        """
        _createDatasets_

        Create All the output datasets found in the workflow spec instance
        provided

        """
        workflowSpec.payload.operate(
            _CreateDatasetOperator(self.dbs, workflowSpec)
            )
        return
        

    def createMergeDatasets(self, workflowSpec, fastMerge = True):
        """
        _createMergeDatasets_

        Create merge output datasets for a workflow Spec
        Expects a Processing workflow spec from which it will generate the
        merge datasets automatically.

        
        """
        mergeSpec = createMergeDatasetWorkflow(workflowSpec, fastMerge)        
        mergeSpec.payload.operate(
            _CreateMergeDatasetOperator(self.dbs, workflowSpec)
            )
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
            dbsFiles = DBSWriterObjects.createDBSFiles(outFile,
                                                       fwkJobRep.jobType)
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
            
            fileBlock = DBSWriterObjects.getDBSFileBlock(
                self.dbs,
                procDataset,
                fileList.seName)
            

            if fwkJobRep.jobType == "Merge":
                #  //
                # // Merge files
                #//
                for mergedFile in fileList:
                    mergedFile['Block'] = fileBlock
                    self.dbs.insertMergedFile(mergedFile['ParentList'],
                                              mergedFile)
                
            else:
                #  //
                # // Processing files
                #//
                self.dbs.insertFiles(procDataset, list(fileList), fileBlock)
                
        return
