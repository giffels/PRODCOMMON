#!/usr/bin/env python
"""
_Tier0WorkflowMaker_

Object that can be used to construct a tier 0 workflow spec and manipulate it
to add details


!!!!!!!!!!!!!!!!!!NOTE!!!!!!!!!!!!!!!!!

Fast first pass, good only for conversion for global data taking
Needs refinement...

"""


import time
import os

from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker, WorkflowMakerError

import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase




        


class Tier0WorkflowMaker(WorkflowMaker):
    """
    _Tier0WorkflowMaker_

    Specialisation of basic MC workflow maker for Tier 0 data handling.
    
    """
    def __init__(self, requestId, channel, label):
        WorkflowMaker.__init__(self, requestId, channel, label)
        self.unmergedDataset = False
        self.runNumber = None

    def makeUnmergedDataset(self):
        """
        _makeUnmergedDataset_

        Call this method if you want to make unmerged datasets.
        Default is to not make unmerged datasets

        """
        self.unmergedDataset = True



    def setRunNumber(self, runNumber):
        """
        _setRunNumber_

        Temp hack used to generate LFNs for conversion

        """
        self.runNumber = runNumber


    def makeWorkflow(self):
        """
        _makeWorkflow_

        Call this method to create the workflow spec instance when
        done

        """
        self._Validate()

        #  //
        # // Input Dataset required for Tier0
        #//
    
        inputDataset = self.cmsRunNode.addInputDataset(
            self.inputDataset['Primary'],
            self.inputDataset['Processed']
            )
        inputDataset["DataTier"] = self.inputDataset['DataTier']
        for keyname in [
            'SplitType',
            'SplitSize',
            'OnlySites',
            'OnlyBlocks',
            'OnlyClosedBlocks',
            ]:
            if self.inputDataset[keyname] != None:
                self.workflow.parameters[keyname] = self.inputDataset[keyname]
                
        
        #  //
        # // Extract dataset info from cfg
        #//
        for outModName in self.configuration.outputModules.keys():
            moduleInstance = self.configuration.getOutputModule(outModName)
            #  //
            # // Data Tier same as input
            #//
            dataTier = self.inputDataset['DataTier']
            #  //
            # // Output primary dataset same as input primary
            #//
            primaryName = self.inputDataset['Primary']

            #  //
            # // Output processed dataset
            #//  (Note we pass way more info than is used, since
            #  //conventions have a tendency to change in CMS...
            # //
            #//
            processedName = DatasetConventions.tier0ProcessedDatasetName(
                Version = self.cmsswVersion,
                InputPrimaryDataset = self.inputDataset['Primary'],
                InputProcessedDataset = self.inputDataset['Processed'],
                Label = self.label,
                Group = self.group,
                RequestId = self.requestId,
                Unmerged = self.unmergedDataset
                )
            
            dataTier = DatasetConventions.checkDataTier(dataTier)
            
            moduleInstance['primaryDataset'] = primaryName
            moduleInstance['processedDataset'] = processedName

            outDS = self.cmsRunNode.addOutputDataset(primaryName, 
                                                     processedName,
                                                     outModName)
            
            outDS['DataTier'] = dataTier
            outDS["ApplicationName"] = \
                                     self.cmsRunNode.application["Executable"]
            outDS["ApplicationFamily"] = outModName
            outDS["PhysicsGroup"] = self.group
            outDS["ApplicationFamily"] = outModName


            if self.inputDataset['IsUsed']:
                outDS['ParentDataset'] = self.inputDataset['DatasetName']
                
            if self.options['FakeHash']:
                guid = makeUUID()
                outDS['PSetHash'] = "hash=%s;guid=%s" % (self.psetHash,
                                                         guid)
            else:
                outDS['PSetHash'] = self.psetHash

            
        #  //
        # // Add Stage Out node
        #//
        WorkflowTools.addStageOutNode(self.cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(self.cmsRunNode, "logArchive")

        #  //
        # // generate tier0 LFN bases for this workflow
        #//
        tier0LFN = self.makeTier0LFN()

        self.workflow.parameters['MergedLFNBase'] = tier0LFN
        self.workflow.parameters['UnmergedLFNBase'] = tier0LFN
        
        return self.workflow


    def makeTier0LFN(self):
        """
        _makeTier0LFN_

        Generate an LFN for this workflow
        
        """
        #  //
        # // Remove stream name from primary dataset name
        #//
        primaryDataset = self.inputDataset['Primary']
        primaryDatasetElements = primaryDataset.rsplit("-",1)
        if ( len(primaryDatasetElements) > 1 ):
            datasetName =  primaryDatasetElements[0]
            streamName = primaryDatasetElements[1]
            lfn = "/store/data/%s" % datasetName
            lfn += "/%s" % streamName
        else:
            lfn = "/store/data/%s" % primaryDataset

        runString = str(self.runNumber).zfill(9)
        runFragment = "/%s/%s/%s" % (runString[0:3],
                                     runString[3:6],
                                     runString[6:9])
        lfn += runFragment
        lfn += "/"
        return lfn
        

    def _Validate(self):
        """
        _Validate_

        Private method to test all options are set.

        Throws a WorkflowMakerError if any problems found

        """
        WorkflowMaker._Validate(self)

        if self.runNumber == None:
            msg = "runNumber Attribute Not Set"
            raise WorkflowMakerError(msg)
        
        

        return

