#!/usr/bin/env python
"""
_MergeTools_

Common tools for generating and managing merging workflows

"""

import copy
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.DatasetInfo import DatasetInfo


class ProcToMerge:
    """
    _ProcToMerge_

    Functor that operates on all datasets in a payload node
    to convert them into merged datasets

    """
    def __init__(self, fastMerge = True):
        self.mergeModuleName = "Merged"
        self.appName = "cmsRun"
        if fastMerge:
            self.mergeModuleName = "EdmFastMerge"
            self.appName = "EdmFastMerge"
        
                 
    def __call__(self, node):
        """
        _operator(node)_

        Operate on all output datasets in a Payload Node

        """
        newDatasets = []
        for dataset in node._OutputDatasets:
            if dataset.has_key("NoMerge"):
                #  //
                # // If we need to avoid merging some datasets we
                #//  can add a NoMerge key and this will ignore it
                continue
            newDataset = DatasetInfo()
            newDataset.update(dataset)
            newDataset["ApplicationFamily"] = self.mergeModuleName
            newDataset["ApplicationName"] = self.appName
            procName = dataset["ProcessedDataset"]
            if procName.endswith("-unmerged"):
                procName = procName.replace("-unmerged", "")
            else:
                procName = "%s-merged" % procName
            newDataset["ProcessedDataset"] = procName
            newDataset["ParentDataset"] = dataset.name()

            newDatasets.append(newDataset)

        node._OutputDatasets = []
        node._OutputDatasets = newDatasets
        return


def createMergeDatasetWorkflow(procSpec, isFastMerge = True):
    """
    _createMergeDatasetWorkflow_

    Given a Processing Workflow Spec, generate a Merge Workflow
    spec from it that can be used to generate the merged datasets
    in DBS.

    """
    newSpec = copy.deepcopy(procSpec)
    operator = ProcToMerge(isFastMerge)
    newSpec.payload.operate(operator)
    return newSpec


    
    
