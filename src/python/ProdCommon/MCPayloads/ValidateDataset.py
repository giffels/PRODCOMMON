#!/usr/bin/env python
"""
ValidateDataset

Tools for checking datasets are valid in DBS

"""
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMakerError

def validateDataset( datasetPath, dbsUrl):
    """
    _validateDataset_
    
    Util method to check that the datasetPath provided
    exists in the dbsUrl provided
    
    """
    
    datasetDetails = DatasetConventions.parseDatasetPath(datasetPath)
    for key in ['Primary', 'DataTier', 'Processed']:
        if datasetDetails[key] == None:
            msg = "Invalid Dataset Name: \n ==> %s\n" % datasetPath
            msg += "Does not contain %s information" % key
            raise WorkflowMakerError(msg)
                

    datasets = []
    try:
        reader = DBSReader(dbsUrl)
        datasets = reader.matchProcessedDatasets(
            datasetDetails['Primary'],
            datasetDetails['DataTier'],
            datasetDetails['Processed'])
    except Exception, ex:
        msg = "Error calling DBS to validate dataset:\n%s\n" % datasetPath
        msg += str(ex)
        raise WorkflowMakerError(msg)
    

    if len(datasets) == 0:
        msg = "No matching datasets found for path:\n ==> %s\n" % (
            datasetPath,
            )
        msg += " In DBS Instance:\n ==> %s\n" % dbsUrl
        raise WorkflowMakerError(msg)
    
    return 
