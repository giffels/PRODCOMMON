#!/usr/bin/env python
"""
_DatasetTools_

Tools for extracting Dataset information from individual PayloadNodes or
JobSpecNodes


"""

from ProdCommon.MCPayloads.DatasetInfo import DatasetInfo
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

def _sortDatasets(datasets):
    from WMCore.Algorithms.TreeSort import TreeSort
    name = lambda x: x.name()
    parents = lambda x: x['ParentDataset']
    return TreeSort(name, parents, datasets).sort()


def getOutputDatasets(payloadNode, sorted = False):
    """
    _getOutputDatasets_

    Extract all the information about output datasets from the
    payloadNode object provided.

    Returns a list of DatasetInfo objects including App details
    from the node.

    """
    result = []
    
    for item in payloadNode._OutputDatasets:
        resultEntry = DatasetInfo()
        resultEntry.update(item)
        resultEntry["ApplicationName"] = payloadNode.application['Executable']
        resultEntry["ApplicationProject"] = payloadNode.application['Project']
        resultEntry["ApplicationVersion"] = payloadNode.application['Version']
        result.append(resultEntry)
    
    if sorted:
        result = _sortDatasets(result)
    return result

def getOutputDatasetsWithPSet(payloadNode, sorted = False):
    """
    _getOutputDatasetsWithPSet_

    Extract all the information about output datasets from the
    payloadNode object provided, including the {{}} format PSet cfg

    Returns a list of DatasetInfo objects including App details
    from the node.

    """
    result = []
    
    for item in payloadNode._OutputDatasets:
        resultEntry = DatasetInfo()
        resultEntry.update(item)
        resultEntry["ApplicationName"] = payloadNode.application['Executable']
        resultEntry["ApplicationProject"] = payloadNode.application['Project']
        resultEntry["ApplicationVersion"] = payloadNode.application['Version']
        resultEntry["ApplicationFamily"] = item.get("OutputModuleName", "AppFamily")
        
        try:
            config = payloadNode.cfgInterface
            psetStr = config.originalContent()
            resultEntry['PSetContent'] = psetStr
            resultEntry['Conditions'] = config.conditionsTag
        except Exception, ex:
            resultEntry['PSetContent'] = None
        
        result.append(resultEntry)
       
    if sorted:
        result = _sortDatasets(result)
    return result


def getPileupDatasets(payloadNode):
    """
    _getPileupDatasets_

    Extract all pileup dataset info from the node provided.
    Returns a list of dataset info objects

    """
    result = []
    for item in payloadNode._PileupDatasets:
        resultEntry = DatasetInfo()
        resultEntry.update(item)
        resultEntry['NodeName'] = payloadNode.name
        result.append(resultEntry)
    return result

def getInputDatasets(payloadNode, sorted = False):
    """
    _getInputDatasets_

    Extract all the information about input datasets from the
    payloadNode object provided.

    Returns a list of DatasetInfo objects including App details
    from the node.

    """
    result = []
    for item in payloadNode._InputDatasets:
        resultEntry = DatasetInfo()
        resultEntry.update(item)
        result.append(resultEntry)
    if sorted:
        result = _sortDatasets(result)
    return result

class Accumulator:
    """
    _Accumulator_

    Callable Object that collects output from
    an operator used to traverse a node tree

    """
    def __init__(self, operatorRef):
        self.operator = operatorRef
        self.result = []


    def __call__(self, payloadNodeRef):
        """
        _operator()_

        Invoke the operator on the payloadNodeRef and
        keep the output
        """
        self.result.extend(self.operator(payloadNodeRef))
        return

def getOutputDatasetsFromTree(topPayloadNode, sorted = False):
    """
    _getOutputDatasetsFromTree_

    Traverse a PayloadNode tree with the getOutputDatasets method
    and accumulate the datasets from all nodes, to be returned as a list

    """
    accum = Accumulator(getOutputDatasets)
    topPayloadNode.operate(accum)
    result = accum.result
    if sorted:
        result = _sortDatasets(result)
    return result


def getInputDatasetsFromTree(topPayloadNode, sorted = False):
    """
    _getInputDatasetsFromTree_

    Traverse a PayloadNode tree with the getInputDatasets method
    and accumulate the datasets from all nodes, to be returned as a list

    """
    accum = Accumulator(getInputDatasets)
    topPayloadNode.operate(accum)
    result = accum.result
    if sorted:
        result = _sortDatasets(result)
    return result


def getOutputDatasetsWithPSetFromTree(topPayloadNode, sorted = False):
    """
    _getOutputDatasetsWithPSetFromTree_

    Traverse a PayloadNode tree with the getOutputDatasets method
    and accumulate the datasets from all nodes, to be returned as a list
    Information should include the {{}} format PSet as PSetContent key
    """
    accum = Accumulator(getOutputDatasetsWithPSet)
    topPayloadNode.operate(accum)
    result = accum.result
    if sorted:
        result = _sortDatasets(result)
    return result


def getPileupDatasetsFromTree(topPayloadNode, sorted = False):
    """
    _getPileupDatasetsFromTree_

    Traverse a PayloadNode tree with the getPileupDatasets method
    and accumulate the datasets from all nodes, to be returned as a list

    """
    accum = Accumulator(getPileupDatasets)
    topPayloadNode.operate(accum)
    result = accum.result
    if sorted:
        result = _sortDatasets(result)
    return result
    

def getOutputDatasetDetails(jobSpecNode, sorted = False):
    """
    _getOutputDatasetDetails_

    Extract the set of output datasets from the JobSpecNode instance
    and also add any details about the output module that
    goes with it

    """
    datasets = getOutputDatasets(jobSpecNode, sorted = sorted)
    jobSpecNode.loadConfiguration()
    outMods = jobSpecNode.extractOutputModules()
    for dataset in datasets:
        dataset.setdefault("Catalog", None)
        dataset.setdefault("LogicalFileName", None)
        dataset.setdefault("PhysicalFileName", None)
        modName = dataset["OutputModuleName"]
        if modName ==  None:
            continue

        if modName not in outMods:
            continue
        outputModule = jobSpecNode.cfgInterface.outputModules[modName]
        dataset['Catalog'] = outputModule['catalog']
        dataset['LogicalFileName'] = outputModule['logicalFileName']
        dataset['PhysicalFileName'] = outputModule['fileName']
    return datasets

        
    
def getOutputDatasetDetailsFromTree(topJobSpecNode, sorted = False):
    """
    _getOutputDatasetDetailsFromTree_

    Extract the set of output datasets from the JobSpecNode and
    include all of the details about corresponding output modules
    for each dataset

    """
    accum = Accumulator(getOutputDatasetDetails)
    topJobSpecNode.operate(accum)
    result = accum.result
    if sorted:
        result = _sortDatasets(result)
    return result
