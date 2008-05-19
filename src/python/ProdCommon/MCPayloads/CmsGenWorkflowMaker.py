#!/usr/bin/env python
"""
_WorkflowMaker_

Objects that can be used to construct a workflow spec and manipulate it
to add details


"""


import time
import os
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.Core.ProdException import ProdException

import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase

from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker, WorkflowMakerError



        


class CmsGenWorkflowMaker(WorkflowMaker):
    """
    _CmsGenWorkflowMaker_

    Basic MC workflow maker for PR to use to create workflow spec files that include a cmsGen step
    
    """
    def __init__(self, requestId, channel, label):
        WorkflowMaker.__init__(self, requestId, channel, label)

        #  //
        # // cmsGen params
        #//
        self.cmsGenParameters = {
            "generator" : None,
            "executable" : None,
            "version" : None,
            }
        self.cmsGenDataset = {}
        self.cmsGenDataset['Primary'] = None
        self.cmsGenDataset['Processed'] = None
        self.cmsGenDataset['DataTier'] = None


        #  //
        # // first node is cmsGen
        #//
        self.cmsGenNode = self.workflow.payload
        self.cmsGenNode.name = "cmsGen1"
        self.cmsGenNode.type = "CmsGen"
        self.cmsGenNode.configuration = None

        #  //
        # // second node is cmsRun
        #//
        self.cmsRunNode = self.cmsGenNode.newNode("cmsRun1")
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.addInputLink("cmsGen1", "cmsGen",
                                     "source", True)
        self.cmsRunNodes = [self.cmsRunNode]
        
        return


    def setCmsGenCMSSWVersion(self, version):
        """
        _setCMSSWVersion_

        """
        self.cmsGenNode.application['Executable'] = "cmsGen"
        self.cmsGenNode.application['Project'] = "CMSSW"
        self.cmsGenNode.application['Architecture'] = ""
        self.cmsGenNode.application['Version'] = version

        return



    def setCmsGenConfiguration(self, bigStringWeDontLookAt):
        """
        _setCmsGenConfiguration_

        """
        self.cmsGenNode.configuration = bigStringWeDontLookAt
        return

    def setCmsGenParameters(self, **args):
        """
        _setCmsGenParameters_

        Pack paramaters into the applicationControls dictionary
        of the cmsGen Node

        """
        self.cmsGenNode.applicationControls.update(args)
        return
        
        
    def addCmsGenSelectionEfficiency(self, selectionEff):
        """
        _addSelectionEfficiency_
                                                                                                           
        Do we have a selection efficiency?
                                                                                                           
        """
                                                                                                           
        self.cmsGenNode.applicationControls["SelectionEfficiency"] = \
                                                             selectionEff
        return


    def _Validate(self):
        """
        _ValidateCmasGen_

        Private method to test all options are set.

        Throws a WorkflowMakerError if any problems found

        """

        
        if self.cmsGenNode.applicationControls.get("generator", None) == None:
            msg = "No cmsGen generator option provided"
            raise RuntimeError, msg
        
        return WorkflowMaker._Validate(self)
        
