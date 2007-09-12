#!/usr/bin/env python
"""
_DatasetJobFactory_

Given a processing workflow, generate a complete set of
job specs for it.


"""

import os
import logging


from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from PileupTools.PileupDataset import PileupDataset, createPileupDatasets, getPileupSites
from ProdAgentCore.Configuration import loadProdAgentConfiguration


from ProdCommon.DataMgmt.JobSplit.SplitterMaker import splitDatasetByEvents
from ProdCommon.DataMgmt.JobSplit.SplitterMaker import splitDatasetByFiles
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter


class FilterSites:
    """
    Functor for filtering site lists
    """
    def __init__(self, allowed):
        self.allowed = allowed
    def __call__(self, object):
        return object in self.allowed



class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance

    """
    def __init__(self):
        dict.__init__(self)


    def __call__(self, payloadNode):
        if payloadNode.cfgInterface != None:
            generator = CfgGenerator(payloadNode.cfgInterface, False,
                                     payloadNode.applicationControls)
            self[payloadNode.name] = generator
            return
            
        if payloadNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(payloadNode.configuration, True,
                                         payloadNode.applicationControls)
            self[payloadNode.name] = generator
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return
    
        



class DatasetJobFactory:
    """
    _DatasetJobFactory_

    Working from a processing Workflow template, generate
    a complete set of job spec files from it

    """
    def __init__(self, workflowSpec, workingDir, dbsUrl, **args):
        self.workingDir = workingDir
        
        self.workflowSpec = workflowSpec
        self.dbsUrl = dbsUrl
        self.count = args.get("InitialRun", 0)


        self.currentJob = None
        self.currentJobDef = None
        
        
        self.onlyClosedBlocks = False
        if  self.workflowSpec.parameters.has_key("OnlyClosedBlocks"):
            onlyClosed =  str(
                self.workflowSpec.parameters["OnlyClosedBlocks"]).lower()
            if onlyClosed == "true":
                self.onlyClosedBlocks = True

        self.allowedBlocks = []
        self.allowedSites = []

        self.splitType = \
                self.workflowSpec.parameters.get("SplitType", "file").lower()
        self.splitSize = int(self.workflowSpec.parameters.get("SplitSize", 1))

        self.generators = GeneratorMaker()
        self.generators(self.workflowSpec.payload)

        self.pileupDatasets = {}
        
        #  //
        # // Does the workflow contain a site restriction??
        #//
        siteRestriction = \
           self.workflowSpec.parameters.get("OnlySites", None)          
        if siteRestriction != None:
            #  //
            # // restriction on sites present, populate allowedSites list
            #//
            msg = "Site restriction provided in Workflow Spec:\n"
            msg += "%s\n" % siteRestriction
            siteList = siteRestriction.split(",")
            for site in siteList:
                if len(site.strip() ) > 0:
                    self.allowedSites.append(site.strip())

        blockRestriction = \
             self.workflowSpec.parameters.get("OnlyBlocks", None)
        if blockRestriction != None:
            #  //
            # // restriction on blocks present, populate allowedBlocks list
            #//
            msg = "Block restriction provided in Workflow Spec:\n"
            msg += "%s\n" % blockRestriction
            blockList = blockRestriction.split(",")
            for block in blockList:
                if len(block.strip() ) > 0:
                    self.allowedBlocks.append(block.strip())

            
        #  //
        # // Cache Area for JobSpecs
        #//
        self.specCache = os.path.join(
            self.workingDir,
            "%s-Cache" %self.workflowSpec.workflowName())
        if not os.path.exists(self.specCache):
            os.makedirs(self.specCache)
            
        
    def __call__(self):
        """
        _operator()_

        Generate the appropriate number of job specs based on the
        workflow and settings in the file

        """
        ### Need to test these still work OK
        ###TODO:self.loadPileupDatasets()
        ###TODO:self.loadPileupSites()
        
        result = []
        for jobDef in self.processDataset():
            newJobSpec = self.createJobSpec(jobDef)
            jobDict = {
                "JobSpecId" : self.currentJob,
                "JobSpecFile": newJobSpec,
                "JobType" : "Processing",
                "WorkflowSpecId" : self.workflowSpec.workflowName(),
                "WorkflowPriority" : 10,
                }
            result.append(jobDict)
            self.count += 1
            
        return result


    def loadPileupDatasets(self):
        """
        _loadPileupDatasets_
        
        Are we dealing with pileup? If so pull in the file list
        
        """
        puDatasets = self.workflowSpec.pileupDatasets()
        if len(puDatasets) > 0:
            logging.info("Found %s Pileup Datasets for Workflow: %s" % (
                len(puDatasets), self.workflowSpec.workflowName(),
                ))
            self.pileupDatasets = createPileupDatasets(self.workflowSpec)
        return

    def loadPileupSites(self):
        """
        _loadPileupSites_
                                                                                                              
        Are we dealing with pileup? If so pull in the site list
                                                                                                              
        """
        sites = []
        puDatasets = self.workflowSpec.pileupDatasets()
        if len(puDatasets) > 0:
            logging.info("Found %s Pileup Datasets for Workflow: %s" % (
                len(puDatasets), self.workflowSpec.workflowName(),
                ))
            sites = getPileupSites(self.workflowSpec)
        return sites

    def processDataset(self):
        """
        _processDataset_

        Import the Dataset contents and create a set of jobs from it

        """
        
        #  //
        # // Now create the job definitions
        #//
        logging.debug("SplitSize = %s" % self.splitSize)
        logging.debug("SplitType = %s" % self.splitType)

        if self.splitType == "event":
            jobDefs = splitDatasetByEvents(self.inputDataset(),
                                           self.dbsUrl, self.splitSize,
                                           self.onlyClosedBlocks,
                                           self.allowedSites,
                                           self.allowedBlocks)
        else:
            jobDefs = splitDatasetByFiles(self.inputDataset(),
                                          self.dbsUrl, self.splitSize,
                                          self.onlyClosedBlocks,
                                          self.allowedSites,
                                          self.allowedBlocks)
            
        return jobDefs
    
        


    def inputDataset(self):
        """
        _inputDataset_

        Extract the input Dataset from this workflow

        """
        topNode = self.workflowSpec.payload
        try:
            inputDataset = topNode._InputDatasets[-1]
        except StandardError, ex:
            msg = "Error extracting input dataset from Workflow:\n"
            msg += str(ex)
            logging.error(msg)
            return None

        return inputDataset.name()
        
            
    def createJobSpec(self, jobDef):
        """
        _createJobSpec_

        Load the WorkflowSpec object and generate a JobSpec from it

        """
        
        jobSpec = self.workflowSpec.createJobSpec()
        jobName = "%s-%s" % (
            self.workflowSpec.workflowName(),
            self.count,
            )
        self.currentJob = jobName
        self.currentJobDef = jobDef
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = self.count


        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(self.generateJobConfig)


        specCacheDir =  os.path.join(
            self.specCache, str(self.count // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)
        
        

        #  //
        # // Add site pref if set
        #//
        for site in jobDef['SENames']:
            jobSpec.addWhitelistSite(site)
            
        
        jobSpec.save(jobSpecFile)
        
        return jobSpecFile
        
        
    def generateJobConfig(self, jobSpecNode):
        """
        _generateJobConfig_
        
        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File
                
        """
        if jobSpecNode.name not in self.generators.keys():
            return

        generator = self.generators[jobSpecNode.name]
        
        
        maxEvents = self.currentJobDef.get("MaxEvents", None)
        skipEvents = self.currentJobDef.get("SkipEvents", None)

        args = {
            'fileNames' : self.currentJobDef['LFNS'],
            }
            
        if self.splitType == "file":
           maxEvents = -1
        if maxEvents != None:
            args['maxEvents'] = maxEvents
        if skipEvents != None:
            args['skipEvents'] = skipEvents

        jobCfg = generator(self.currentJob, **args)
        #  //
        # // Is there pileup for this node?
        #//
        if self.pileupDatasets.has_key(jobSpecNode.name):
            puDataset = self.pileupDatasets[jobSpecNode.name]
            logging.debug("Node: %s has a pileup dataset: %s" % (
                jobSpecNode.name,  puDataset.dataset,
                ))
            
            fileList = puDataset.getPileupFiles()
            jobCfg.pileupFiles = fileList
            

        
        jobSpecNode.cfgInterface = jobCfg
        return
    


    

