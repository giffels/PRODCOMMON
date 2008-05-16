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
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.MCPayloads.UUID import makeUUID





class WorkflowMakerError(ProdException):
    """
    _WorkflowMakerError_

    All Exceptions from the WorkflowMaker are this kind
    
    """
    def __init__(self, message, **data):
        ProdException.__init__(self, message, 10000, **data)


        


class WorkflowMaker:
    """
    _WorkflowMaker_

    Basic MC workflow maker for PR to use to create workflow spec files.
    
    """
    def __init__(self, requestId, channel, label):
        self.requestId = requestId
        self.group = None
        self.label = label
        self.timestamp = int(time.time())
        self.channel = channel
        self.cmsswVersions = []
        self.configurations = []
        self.psetHash = None
        self.acquisitionEra = None
        self.processingVersion = None
        self.conditions = None
        
        self.options = {}
        self.options.setdefault('FakeHash', False)

        self.inputDataset = {}
        self.inputDataset['IsUsed'] = False
        self.inputDataset['DatasetName'] = None
        self.inputDataset['Primary'] = None
        self.inputDataset['Processed'] = None
        self.inputDataset['DataTier'] = None
        #  //
        # // Extra controls over input dataset if required
        #//
        self.inputDataset['SplitType'] = None
        self.inputDataset['SplitSize'] = None
        self.inputDataset['OnlySites'] = None
        self.inputDataset['OnlyBlocks'] = None
        self.inputDataset['OnlyClosedBlocks'] = True

        #  //
        # // Pileup Dataset controls
        #//
        self.pileupDataset = {}
        self.pileupDataset['IsUsed'] = False
        self.pileupDataset['Primary'] = None
        self.pileupDataset['Processed'] = None
        self.pileupDataset['DataTier'] = None
        self.pileupDataset['FilesPerJob'] = 1
        
        #  //
        # // Initialise basic workflow
        #//
        self.workflow = WorkflowSpec()
        self.workflowName = "%s-%s-%s" % (label, channel, requestId)
        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.setRequestCategory("mc")
        self.workflow.setRequestTimestamp(self.timestamp)
        self.workflow.parameters['RequestLabel'] = self.label
        self.workflow.parameters['ProdRequestID'] = self.requestId

        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        
        self.cmsRunNodes = [self.cmsRunNode]


    def chainCmsRunNode(self):
        """
        append a cmsRun config to the current cmsRun node and chain them
        """
        newnode = self.cmsRunNode.newNode("cmsRun%s" % 
                                          (len(self.cmsRunNodes) + 1))
        newnode.type = "CMSSW"
        newnode.addInputLink(self.cmsRunNode.name, 
                             self.configurations[-1].outputModules.keys()[0],
                             'source')
        self.cmsRunNode = newnode
        self.cmsRunNodes.append(newnode)
        #return self.cmsRunNode


    def changeCategory(self, newCategory):
        """
        _changeCategory_

        Change the workflow category from the default mc
        that appears in the LFNs

        """
        self.workflow.setRequestCategory(newCategory)
        return

    def setAcquisitionEra(self,era):
        """
        _setAcquisitionEra_
        
        Sets the AcquisitionEra in the workflow 

        """
        self.workflow.setAcquisitionEra(era)
        self.acquisitionEra=era


    
    def setActivity(self, activity):
        """
        _changeWorkflowType"
        
        Set the workflow type
        i.e. Simulation, Reconstruction, Reprocessing, Skimming
        """
        self.workflow.setActivity(activity)
        return
    

    def setCMSSWVersion(self, version):
        """
        _setCMSSWVersion_

        Set the version of CMSSW to be used

        """
        self.cmsswVersions.append(version)
        self.cmsRunNode.application['Version'] = version
        self.cmsRunNode.application['Executable'] = "cmsRun"
        self.cmsRunNode.application['Project'] = "CMSSW"
        self.cmsRunNode.application['Architecture'] = ""
        return


    def setUserSandbox(self,sandboxloc):
        """
        _setSandbox_
        Sets the location of the user sandbox

        """
        self.cmsRunNode.userSandbox=sandboxloc
        return
    
    
    def setPhysicsGroup(self, group):
        """
        _setPhysicsGroup_

        Physics Group owning the workflow

        """
        self.group = group
        self.workflow.parameters['PhysicsGroup'] = self.group
        return

    
    def setConfiguration(self, cfgFile, **args):
        """
        _setConfiguration_

        Provide the CMSSW configuration to be used.
        By default, assume that cfgFile is a python format string.

        The format & type can be specified using args:

        - Type   : must be "file" or "string" or "instance"
        
        """
        cfgType = args.get("Type", "instance")
        
        
        if cfgType not in ("file", "string", "instance"):
            msg = "Illegal Type for cfg file: %s\n" % cfgType
            msg += "Should be \"file\" or \"string\"\n"
            raise RuntimeError, msg

        cfgContent = cfgFile
        if cfgType == "file":
            cfgContent = file(cfgFile).read()
            cfgType = "string"
            
        if cfgType == "string":
            cfgData = cfgContent
            cfgContent = CMSSWConfig()
            cfgContent.unpack(cfgData)
        
                
        self.cmsRunNode.cfgInterface = cfgContent
        self.configurations.append(cfgContent)
        return


    def setOriginalCfg(self, honkingGreatString):
        """
        _setOriginalCfg_

        Set the original cfg file content that is to be recorded in DBS

        CALL THIS METHOD AFTER setConfiguration
        
        """
        self.cmsRunNode.cfgInterface.originalCfg = honkingGreatString
        return
        
    def setPSetHash(self, hashValue):
        """
        _setPSetHash_

        Set the value for the PSetHash

        """
        self.psetHash = hashValue
        return
        

    
    def addInputDataset(self, datasetPath):
        """
        _addInputDataset_

        If this workflow processes a dataset, set that here

        NOTE: Is possible to also specify
            - Split Type (file or event)
            - Split Size (int)
            - input DBS
        Not sure how many of these we want to use.
        For now, they can be added to the inputDataset dictionary
        """
        datasetBits = DatasetConventions.parseDatasetPath(datasetPath)
        self.inputDataset.update(datasetBits)
        self.inputDataset['IsUsed'] = True
        self.inputDataset['DatasetName'] = datasetPath
        
        return
        

    def addPileupDataset(self, datasetName, filesPerJob = 10):
        """
        _addPileupDataset_

        Add a dataset to provide pileup overlap.
        filesPerJob should be 1 in 99.9 % of cases

        """
        datasetBits = DatasetConventions.parseDatasetPath(datasetName)
        self.pileupDataset.update(datasetBits)
        self.pileupDataset['FilesPerJob'] = filesPerJob
        self.pileupDataset['IsUsed'] = True
        return

    def addFinalDestination(self, *phedexNodeNames):
        """
        _addFinalDestination_

        Add a final destination that can be used to generate
        a PhEDEx subscription so that the data gets transferred to
        some final location.

        NOTE: Do we want to support a list of PhEDEx nodes? Eg CERN + FNAL

        """
        nameList = ""
        for nodeName in phedexNodeNames:
            nameList += "%s," % nodeName
        nameList = nameList[:-1]
        self.workflow.parameters['PhEDExDestination'] = nameList
        return
    
    def addSelectionEfficiency(self, selectionEff):
        """
        _addSelectionEfficiency_

        Do we have a selection efficiency?

        """
        
        self.cmsRunNode.applicationControls["SelectionEfficiency"] = \
                                                             selectionEff
        return
    
    def makeWorkflow(self):
        """
        _makeWorkflow_

        Call this method to create the workflow spec instance when
        done

        """
        self._Validate()

        #  //
        # // Input Dataset?
        #//
        if self.inputDataset['IsUsed']:
            inputDataset = self.cmsRunNodes[0].addInputDataset(
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
        # // Pileup Dataset?
        #//
        if self.pileupDataset['IsUsed']:
            puDataset = self.cmsRunNodes[0].addPileupDataset(
                self.pileupDataset['Primary'],
                self.pileupDataset['DataTier'],
                self.pileupDataset['Processed'])
            puDataset['FilesPerJob'] = self.pileupDataset['FilesPerJob']
            
        
        #  //
        # // Extract dataset info from cfg
        #//
        for outModName in self.configurations[-1].outputModules.keys():
            moduleInstance = self.configurations[-1].getOutputModule(outModName)
            dataTier = moduleInstance['dataTier']
            filterName = moduleInstance["filterName"]
            primaryName = DatasetConventions.primaryDatasetName(
                                    PhysicsChannel = self.channel,
                                    )
            if self.acquisitionEra == None:
              processedName = DatasetConventions.processedDatasetName(
                  Version = self.cmsswVersions[-1],
                  Label = self.label,
                  Group = self.group,
                  FilterName = filterName,
                  RequestId = self.requestId,
                  Unmerged = True
                  )
            else:
              processedName = DatasetConventions.csa08ProcessedDatasetName(
                  AcquisitionEra = self.acquisitionEra,
                  Conditions = self.workflow.parameters['Conditions'],
                  ProcessingVersion = self.workflow.parameters['ProcessingVersion'],
                  FilterName = filterName,
                  Unmerged = True
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
        WorkflowTools.generateFilenames(self.workflow)
        
        
        return self.workflow



    def _Validate(self):
        """
        _Validate_

        Private method to test all options are set.

        Throws a WorkflowMakerError if any problems found

        """
        notNoneAttrs = [
            "psetHash",
            "requestId",
            "label",
            "group",
            "channel",
            ]
        for attrName in notNoneAttrs:
            value = getattr(self, attrName, None)
            if value == None:
                msg = "Attribute Not Set: %s" % attrName
                raise WorkflowMakerError(msg)
        
        if not len(self.configurations):
            msg = "Attribute Not Set: configurations"
            raise WorkflowMakerError(msg)
            
        if len(self.configurations) != len(self.cmsswVersions):
            msg = "len(self.configurations) != len(self.cmsswVersions)"
            raise WorkflowMakerError(msg)

        return

