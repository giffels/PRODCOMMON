#!/usr/bin/env python
"""
_RedneckParentage_

Utilities for setting up datasets to be related as parents
for cmsRun tasks that produce multiple output datasets.
Remaps brother and sister output modules look like parent and child in
dataset terms.


In a configuration with two output modules, if one has
the setting parentOutputModule in the dataset PSet, then
the parentage of the output for this module will be remapped
to be the named module.

Eg:

process.output1 = cms.OutputModule("PoolOutputModule")
process.output1.dataset = cms.untracked.PSet(
        dataTier = cms.untracked.string('GEN')
    )

process.output2 = cms.OutputModule("PoolOutputModule")
process.output2.dataset = cms.untracked.PSet(
        dataTier = cms.untracked.string('SIM'),
        parentOutputModule = cms.untracked.string("output1")
    )

Will mean that the parent of the dataset produced by
output module 2 will be set to the dataset produced by
output module 1

"""


def redneckParentage(payloadNode):    
    """
    _redneckParentage_

    Act on a CMSSW type PayloadNode instance.
    Search through the output modules for any with the
    parentOutputModule keyword, and if found,
    set the dataset parentage to the dataset produced by the
    labelled output module.

    """
    if payloadNode.type != "CMSSW":
        #  //
        # // Can only do this for CMSSW nodes
        #//
        return
    
    config = payloadNode.cfgInterface
    datasets = {}
    [ datasets.__setitem__(x['OutputModuleName'], x)
      for x in payloadNode._OutputDatasets]

    #  //
    # // Loop over existing output modules
    #//
    for childModName in config.outputModules.keys():
        childModule = config.getOutputModule(childModName)
        parentModName = childModule.get('parentOutputModule', None)
        if parentModName == None:
            #  //
            # // No parent remap needed
            #//
            continue
        #  //
        # // Remap needed, find parent, throw if not present
        #//
        parentModule = config.outputModules.get(parentModName, None)
        if parentModule == None:
            msg = "Output Module: %s declares itself a child of\n" % childModName
            msg +=" Non existent Output Module %s\n" % parentModName
            raise RuntimeError, msg
        #  //
        # //  Set the parent dataset name
        #//
        parentDataset = datasets[parentModName]
        childDataset = datasets[childModName]
        childDataset['ParentDataset'] = parentDataset.name()

    return


def remapParentageForWorkflow(workflow):
    """
    _remapParentageForWorkflow_

    Recursively traverse a workflow's nodes looking
    for parentOutputModule settings and adjust parentage
    where needed
    """
    workflow.payload.operate(redneckParentage)
