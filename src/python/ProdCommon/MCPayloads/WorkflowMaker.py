#!/usr/bin/env python
"""
_WorkflowMaker_

Objects that can be used to construct a workflow spec and manipulate it
to add details


"""

import os
import time

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.Core.ProdException import ProdException

import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.MCPayloads.UUID import makeUUID
from ProdCommon.MCPayloads.RedneckParentage import remapParentageForWorkflow




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
        self.psetHashes = {}
        self.origCfgs = {}
        self.acquisitionEra = None
        self.processingString = None
        self.processingVersion = None
        self.conditions = None

        # turn on use of proper naming convention for datasets
        # should be made the default soon, lets deprecate all the old crap
        self.useProperNamingConventions = False
        
        self.options = {}
        self.options.setdefault('FakeHash', False)

        # Should we use another attribute for setting the output dataset
        # status in DBS?
        self.outputDatasetStatus = 'VALID'

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
        self.pileupDatasets = []
        
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
        self.saveOutputFor = []


    def chainCmsRunNode(self, stageOutIntermediates = False, *outputModules):
        """
        append a cmsRun config to the current cmsRun node and chain them
        """
        if stageOutIntermediates: #Do we want to keep cmsRunNode's products?
            self.saveOutputFor.append(self.cmsRunNode.name)    
        newnode = self.cmsRunNode.newNode("cmsRun%s" % 
                                          (len(self.cmsRunNodes) + 1))
        newnode.type = "CMSSW"
        if not outputModules:
            outputModules = self.configurations[-1].outputModules.keys()
        for outmodule in outputModules:
            newnode.addInputLink(self.cmsRunNode.name, outmodule,
                        'source', AppearStandalone = not stageOutIntermediates)
        self.cmsRunNode = newnode
        self.cmsRunNodes.append(newnode)


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
        return


    def setNamingConventionParameters(self, era, procString, procVers):
        """
        _setNamingConventionParameters_

        Sets AcquisitionEra, ProcessingString and ProcessingVersion

        """
        self.workflow.setAcquisitionEra(era)
        self.workflow.parameters['ProcessingString'] = procString
        self.workflow.parameters['ProcessingVersion'] = procVers
        
        self.acquisitionEra=era
        self.processingString = procString
        self.processingVersion = procVers

        self.useProperNamingConventions = True

        return

    
    def setActivity(self, activity):
        """
        _changeWorkflowType_
        
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
        sep = '\n\n### Next chained config file ###\n\n'
        cfg = ''
        for link in self.cmsRunNode._InputLinks:
            if link['AppearStandalone']:
                prev_config = self.origCfgs.get(link['InputNode'], '')
                if prev_config:
                    cfg = '%s%s%s' % (cfg, prev_config, sep)
        cfg = '%s%s' % (cfg, honkingGreatString)
        self.cmsRunNode.cfgInterface.originalCfg = cfg
        self.origCfgs[self.cmsRunNode.name] = cfg
        return
        
    def setPSetHash(self, hashValue):
        """
        _setPSetHash_

        Set the value for the PSetHash
        
        If any InputLinks are present their pset hashes are prepended

        """
        hash = ''
        for link in self.cmsRunNode._InputLinks:
            if link['AppearStandalone']:
                prev_node_hash = self.psetHashes.get(link['InputNode'], None)
                if prev_node_hash:  # cmsGen nodes will be missing
                    hash = '%s%s_' % (hash, prev_node_hash)
        hash = '%s%s' % (hash, hashValue)
        self.psetHashes[self.cmsRunNode.name] = hash                           
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
        

    def addPileupDataset(self, datasetName, filesPerJob = 10,
            targetModule=None):
        """
        _addPileupDataset_

        Add a dataset to provide pileup overlap.
        filesPerJob should be 1 in 99.9 % of cases

        """
        pileupDataset = {}
        pileupDataset['Primary'] = None
        pileupDataset['Processed'] = None
        pileupDataset['DataTier'] = None
        datasetBits = DatasetConventions.parseDatasetPath(datasetName)
        pileupDataset.update(datasetBits)
        pileupDataset['FilesPerJob'] = filesPerJob
        # Target module coould be 'MixingModule' or 'DataMixingModule' for
        # the moment. If None, MixingModule will be used.
        pileupDataset['TargetModule'] = targetModule
        self.pileupDatasets.append(pileupDataset)
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

    def setOutputDatasetDbsStatus(self, status):
        """
        _setOutputDatasetDbsStatus_

        The output datasets will have this status in the field dataset.status.
        This value will be use when registering the output dataset in DBS.

        Only two values are acepted:
            - VALID
            - PRODUCTION

        """
        
        if status in ('VALID', 'PRODUCTION'):
            self.outputDatasetStatus = status

        return

    def makeWorkflow(self):
        """
        _makeWorkflow_

        Call this method to create the workflow spec instance when
        done

        """
        self._Validate()
        
        #  //
        # // Add Stage Out node
        #//
        self.saveOutputFor.append(self.cmsRunNode.name)
        WorkflowTools.addStageOutNode(self.cmsRunNode,
                        "stageOut1", *self.saveOutputFor)
        WorkflowTools.addLogArchNode(self.cmsRunNode, "logArchive")

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
        # // Pileup Datasets?
        #//
        for pileupDataset in self.pileupDatasets:
            puDataset = self.cmsRunNodes[0].addPileupDataset(
                pileupDataset['Primary'],
                pileupDataset['DataTier'],
                pileupDataset['Processed'])
            puDataset['FilesPerJob'] = pileupDataset['FilesPerJob']
            if pileupDataset['TargetModule'] is not None:
                puDataset['TargetModule'] = pileupDataset['TargetModule']
            
        
        #  //
        # // Extract dataset info from cfg
        #//
        datasets = {}
        datasetsToForward = {}
        for cmsRunNode, config in zip(self.cmsRunNodes, self.configurations):
            
            # Ignore nodes that don't save any output. But keep input dataset
            # in case we need to forward it.
            if cmsRunNode.name not in self.saveOutputFor:
                # Store parent dataset in case we need to forward it.
                if self.inputDataset['IsUsed'] and \
                                            cmsRunNode == self.cmsRunNodes[0]:
                    datasetsToForward[cmsRunNode.name] = \
                                            self.inputDataset['DatasetName']
                elif cmsRunNode != self.cmsRunNodes[0]:
                    for inputLink in cmsRunNode._InputLinks:
                        # If the previous cmsRunNode stages out, pull down the
                        # dataset it produced.
                        if not inputLink["AppearStandalone"]:
                            # TODO: Wont work if more than one InputLink exists
                            datasetsToForward[cmsRunNode.name] = \
                                datasets['%s:%s' % (inputLink['InputNode'],
                                inputLink['OutputModule'])]
                        # If the previous cmsRunNode does not stage out, then
                        # use it's parent.
                        else:
                            # TODO: Wont work if more than one InputLink exists
                            datasetsToForward[cmsRunNode.name] = \
                                datasetsToForward[inputLink['InputNode']]
                continue
            
            for outModName in config.outputModules.keys():
                moduleInstance = config.getOutputModule(outModName)
                dataTier = moduleInstance['dataTier']
                filterName = moduleInstance["filterName"]
                primaryName = DatasetConventions.primaryDatasetName(
                                        PhysicsChannel = self.channel,
                                        )

                if self.useProperNamingConventions:
                    if self.processingString and filterName:
                        processingString = "_".join((self.processingString, filterName))
                    elif self.processingString:
                        processingString = self.processingString
                    elif filterName:
                        processingString = filterName
                    else:
                        processingString = None
                    processedName = DatasetConventions.properProcessedDatasetName(
                        AcquisitionEra = self.acquisitionEra,
                        ProcessingString = processingString,
                        ProcessingVersion = self.processingVersion,
                        Unmerged = True
                        )
                elif self.acquisitionEra == None:
                    processedName = DatasetConventions.processedDatasetName(
                        Version = cmsRunNode.application['Version'],
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
    
                outDS = cmsRunNode.addOutputDataset(primaryName, 
                                                         processedName,
                                                         outModName)

                outDS['Status'] = self.outputDatasetStatus                
                outDS['DataTier'] = dataTier
                outDS["ApplicationName"] = \
                                         cmsRunNode.application["Executable"]
                outDS["ApplicationFamily"] = outModName
                outDS["PhysicsGroup"] = self.group
    
                # check for input dataset for first node
                if self.inputDataset['IsUsed'] and cmsRunNode == self.cmsRunNodes[0]:
                    outDS['ParentDataset'] = self.inputDataset['DatasetName']
                # check for staged out intermediates
                elif cmsRunNode != self.cmsRunNodes[0]:
                    for inputLink in cmsRunNode._InputLinks:
                        if not inputLink["AppearStandalone"]:
                            # TODO: Wont work if more than one InputLink exists
                            outDS['ParentDataset'] = datasets['%s:%s' % (inputLink['InputNode'],
                                                                    inputLink['OutputModule'])]
                        elif datasetsToForward.get(
                                inputLink['InputNode']) is not None:
                            outDS['ParentDataset'] = \
                                    datasetsToForward[inputLink['InputNode']]

                if self.options['FakeHash']:
                    guid = makeUUID()
                    outDS['PSetHash'] = "hash=%s;guid=%s" % \
                            (self.psetHashes[cmsRunNode.name], guid)
                else:
                    outDS['PSetHash'] = self.psetHashes[cmsRunNode.name]

                # record output in case used as input to a later node
                datasets['%s:%s' % (cmsRunNode.name, outModName)] = \
                                "/%s/%s/%s" % ( outDS['PrimaryDataset'],
                                                  outDS['ProcessedDataset'],
                                                  outDS['DataTier'])

        # optionally remap sibling relationships to parent-child (i.e HLTDEBUG)
        remapParentageForWorkflow(self.workflow)
        WorkflowTools.generateFilenames(self.workflow)

        return self.workflow



    def _Validate(self):
        """
        _Validate_

        Private method to test all options are set.

        Throws a WorkflowMakerError if any problems found

        """
        notNoneAttrs = [
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

