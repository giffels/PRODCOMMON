#!/usr/bin/env python
"""
_DBSWriter_

Interface object for writing data to DBS

"""

from dbsApi import DbsApi
from dbsException import *
from dbsApiException import *


import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects

from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetsWithPSet






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
        try:
            cfgInt = CfgInterface(pnode.configuration, True)
            cfgMeta = cfgInt.configMetadata()
            cfgMeta['Type'] = self.workflow.parameters["RequestCategory"]      
        except Exception, ex:
            return

        for dataset in datasets:
            primary = DBSWriterObjects.createPrimaryDataset(
                dataset, self.apiRef)
            algo = DBSWriterObjects.createAlgorithm(
                dataset, cfgMeta, self.apiRef)
            processed = DBSWriterObjects.createProcessedDataset(
                primary, algo, dataset, self.apiRef)
            
        return
        
    
    
    
    
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
        

    def insertFiles(self, fwkJobRep):
        """
        _insertFiles_

        Process the files in the FwkJobReport instance and insert
        them into the associated datasets

        """
        for outFile in fwkJobRep.files:
            
            dbsFiles = DBSWriterObjects.createDBSFiles(outFile)
            for f in dbsFiles:
                proc = f['Dataset']
                
                
                
