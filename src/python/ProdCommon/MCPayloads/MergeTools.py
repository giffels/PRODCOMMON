#!/usr/bin/env python
"""
_MergeTools_

Common tools for generating and managing merging workflows

"""

import copy
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.DatasetInfo import DatasetInfo
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

class ProcToMerge:
    """
    _ProcToMerge_

    Functor that operates on all datasets in a payload node
    to convert them into merged datasets

    """
    def __init__(self, fastMerge = True):
        self.mergeModuleName = "Merged"
        self.appName = "cmsRun"
        if fastMerge == True:
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
            newDataset["ApplicationVersion"] = node.application['Version']
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



def createMergeJobWorkflow(procSpec, isFastMerge = True, doCleanUp = True):
    """
    _createMergeJobWorkflow_

    Given a Processing Workflow, generate a set of Merge Job
    workflows that can be used to generate actual merge jobs 
    (as opposed to creating datasets like createMergeDatasetWorkflow)

    returns a dictionary of (input, IE MergeSensor watched) dataset name
    to workflow spec instances

    """
    mergeDatasetWF = createMergeDatasetWorkflow(procSpec, isFastMerge)
    mergeDatasets = mergeDatasetWF.outputDatasets()

    results = {}

    procSpecName = procSpec.workflowName()
    

    for dataset in mergeDatasets:
        inputDataset = dataset['ParentDataset']

        newWF = WorkflowSpec()
        newWF.parameters.update(procSpec.parameters)
        newWF.setWorkflowName(procSpecName)
        newWF.parameters['WorkflowType'] = "Merge"
        

        cmsRunNode = newWF.payload
        cmsRunNode.name = "cmsRun1"
        cmsRunNode.type = "CMSSW"
        cmsRunNode.application["Project"] = "CMSSW"
        cmsRunNode.application["Version"] = dataset['ApplicationVersion']
        cmsRunNode.application["Architecture"] = "slc3_ia32_gcc323"

        if isFastMerge == True:
            cmsRunNode.application["Executable"] = "EdmFastMerge"
            outputModuleName = "EdmFastMerge"
        else:
            cmsRunNode.application["Executable"] = "cmsRun"
            outputModuleName = "Merged"

        #  //
        # // Input Dataset
        #//
        datasetBits = DatasetConventions.parseDatasetPath(inputDataset)
        inDataset = cmsRunNode.addInputDataset(datasetBits['Primary'],
                                               datasetBits['Processed'])
        inDataset["DataTier"] = datasetBits['DataTier']

        #  //
        # // Output Dataset
        #//
        
        outputDataset = cmsRunNode.addOutputDataset(
            dataset['PrimaryDataset'], 
            dataset['ProcessedDataset'], 
            outputModuleName)

        outputDataset["DataTier"] = dataset['DataTier']
        outputDataset["PSetHash"] = dataset['PSetHash']

        outputDataset["ApplicationName"] = \
                    cmsRunNode.application["Executable"]
        outputDataset["ApplicationProject"] = \
                    cmsRunNode.application["Project"]
        outputDataset["ApplicationVersion"] = \
                    cmsRunNode.application["Version"]
        outputDataset["ApplicationFamily"] = outputModuleName
        outputDataset["PhysicsGroup"] = \
                      procSpec.parameters.get('PhysicsGroup', None)
        outputDataset['ParentDataset'] = inputDataset
                
        
        #  //
        # // Add Stage Out node
        #//
        WorkflowTools.addStageOutNode(cmsRunNode, "stageOut1")
        if doCleanUp == True:
            WorkflowTools.addCleanUpNode(cmsRunNode, "cleanUp1")

        WorkflowTools.generateFilenames(newWF)

        
        results[inputDataset] = newWF

    return results
