#!/usr/bin/env python
"""
_RequestIterator_

Maintain a Workflow specification, and when prompted,
generate a new concrete job from that workflow

The Workflow is stored as an MCPayloads.WorkflowSpec,
The jobs are JobSpec instances created from the WorkflowSpec

"""

import os
import logging

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdCommon.CMSConfigTools.SeedService import randomSeed


from ProdCommon.DataMgmt.Pileup.PileupDataset import createPileupDatasets




class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance

    """
    def __init__(self):
        dict.__init__(self)


    def __call__(self, payloadNode):
        if payloadNode.type != "CMSSW":
            return

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




class RequestJobFactory:
    """
    _RequestJobFactory_

    Working from a Generic Workflow template, generate
    a set of jobspecs  from it to produce some total event count

    """
    def __init__(self, workflowSpec, workingDir, totalEvents, **args):
        self.workflowSpec = workflowSpec
        self.workingDir = workingDir
        self.count = args.get("InitialRun", 1)
        self.firstEvent = args.get("InitialEvent", 1)
        # The first event sets also the first event read from the MCDB table
        self.skipMCDB = self.firstEvent - 1
        # The first event sets also the first event read from the LHE table
        self.skipLHE = self.firstEvent - 1
        self.overrideFirstEvent = args.get("OverrideFirstEvent", None)
        if self.overrideFirstEvent != None:
            self.overrideFirstEvent = int(self.overrideFirstEvent)
        self.jobnumber = 0
        self.totalEvents = totalEvents
        self.currentJob = None
        self.pileupDatasets = {}
        self.eventsPerJob = args.get("EventsPerJob",  100)
        # For making sure we always skip the same number of events when
        # reading from a MCDB table.
        self.eventsPerMCDBJob = self.eventsPerJob
        # For making sure we always skip the same number of events when
        # reading from a LHE table.
        self.eventsPerLHEJob = self.eventsPerJob
        self.firstNodeCfg = None #Chain job: cfg options needed in next steps

        self.sites = args.get("Sites", [] )
        self.pileupSites = [] # Sites where the Pileup files are
        self.generators = GeneratorMaker()
        self.workflowSpec.payload.operate(self.generators)



        #  //
        # // Cache Area for JobSpecs
        #//
        self.specCache = os.path.join(
            self.workingDir,
            "%s-Cache" %self.workflowSpec.workflowName())
        if not os.path.exists(self.specCache):
            os.makedirs(self.specCache)



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




    def __call__(self):
        """
        _operator()_

        When called generate a new concrete job payload from the
        generic workflow and return it.

        """

        self.loadPileupDatasets()

        result = []
        eventsLeft = self.totalEvents
        
        while eventsLeft:
            
            if eventsLeft < self.eventsPerJob:
                self.eventsPerJob = eventsLeft
            
            newJobSpec = self.createJobSpec()
            jobDict = {
                "JobSpecId" : self.currentJob,
                "JobSpecFile": newJobSpec,
                "JobType" : "Processing",
                "WorkflowSpecId" : self.workflowSpec.workflowName(),
                "WorkflowPriority" : 10,
                }
            result.append(jobDict)
            self.count += 1
            self.jobnumber += 1
            if self.overrideFirstEvent == None:
                self.firstEvent += self.eventsPerJob
            else:
                self.firstEvent = self.overrideFirstEvent
  
            eventsLeft -= self.eventsPerJob
        return result


    def createJobSpec(self):
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
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")
        jobSpec.parameters['RunNumber'] = self.count


        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(self.generateJobConfig)
        jobSpec.payload.operate(self.generateCmsGenConfig)
        self.firstNodeCfg = None # reset after job configured

        if jobSpec.payload.cfgInterface:
            jobSpec.payload.cfgInterface.rawCfg = None
            jobSpec.payload.cfgInterface.originalCfg = None
        
        specCacheDir =  os.path.join(
            self.specCache, str(self.count // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir,
                                   "%s-JobSpec.xml" % jobName)



        #  //
        # // Add site pref if set
        #//
        if self.pileupSites:
            whitelist = self.pileupSites
        else:
            whitelist = self.sites
        [jobSpec.addWhitelistSite(x) for x in whitelist]

        self.pileupSites = [] # Resetting for next jobSpec
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

        useOutputMaxEv = False
        if jobSpecNode.cfgInterface != None:
            outMaxEv = jobSpecNode.cfgInterface.maxEvents['output']
            if outMaxEv != None:
                useOutputMaxEv = True

        if self.firstNodeCfg: # we are n'th job in chain - run over all data
            maxEvents = -1
        else:
            maxEvents = self.eventsPerJob

        # If we are dealing with MCDBSource type, then add a skipEvents
        # paramater to the first step's configuration.
        skipEvents = None
        if jobSpecNode.cfgInterface.sourceType == 'MCDBSource':
            if not self.firstNodeCfg: # we are 1st job in chain, skip events
                efficiency = jobSpecNode.applicationControls.get(
                    "SelectionEfficiency",
                    None)
                if efficiency is None:
                    efficiency = 1
                skipEvents = self.skipMCDB + int(self.jobnumber) * \
                    int(float(self.eventsPerMCDBJob) / float(efficiency))
                msg = "Job processes a MCDB table. Skipping the first "
                msg += "%s events." % skipEvents
                logging.info(msg)

        # If we are dealing with LHESource type, then add a skipEvents
        # paramater to the first step's configuration.
        if jobSpecNode.cfgInterface.sourceType == 'LHESource':
            if not self.firstNodeCfg: # we are 1st job in chain, skip events
                efficiency = jobSpecNode.applicationControls.get(
                    "SelectionEfficiency",
                    None)
                if efficiency is None:
                    efficiency = 1
                skipEvents = self.skipLHE + int(self.jobnumber) * \
                    int(float(self.eventsPerLHEJob) / float(efficiency))
                msg = "Job processes a LHE table. Skipping the first "
                msg += "%s events." % skipEvents
                logging.info(msg)

        if useOutputMaxEv:
            jobCfg = generator(
                self.currentJob,
                maxEventsWritten=maxEvents,
                firstEvent=self.firstEvent,
                firstRun=self.workflowSpec.workflowRunNumber(),
                firstLumi=self.count,
                skipEvents=skipEvents)
        else:
            jobCfg = generator(
                self.currentJob,
                maxEvents=maxEvents,
                firstEvent=self.firstEvent,
                firstRun=self.workflowSpec.workflowRunNumber(),
                firstLumi=self.count,
                skipEvents=skipEvents)



        #  //
        # // Is there pileup for this node?
        #//
        if self.firstNodeCfg: # n'th job in chain - use initial pileup settings
            jobCfg.pileupFiles = self.firstNodeCfg.pileupFiles
            jobCfg.dataMixerFiles = self.firstNodeCfg.dataMixerFiles
        elif self.pileupDatasets.has_key(jobSpecNode.name):
            for pileupDataset in self.pileupDatasets[jobSpecNode.name]:
                #  //
                # // DataMixer pileup
                #//
                if pileupDataset.targetModule == 'DataMixingModule':
                    puDataset = pileupDataset
                    logging.debug("Node: %s has a dataMix pileup dataset: %s" \
                        % (jobSpecNode.name,  puDataset.dataset))
                    #  //
                    # // In event of being no site whitelist, should we
                    #//  restrict the site whitelist to the list of sites
                    #  //containing the PU sample?
                    # // Ans. Yes. /diego
                    #//
                    fileList, self.pileupSites = \
                        puDataset.getPileupFiles(*self.sites)
                    jobCfg.dataMixerFiles = fileList
                    logging.debug("DataMix pileup Files Added: %s" % fileList)
                #  //
                # // Regular pileup
                #//
                elif pileupDataset.targetModule in (None, 'MixingModule'):
                    puDataset = pileupDataset
                    logging.debug("Node: %s has a pileup dataset: %s" % (
                        jobSpecNode.name,  puDataset.dataset
                    ))
                    #  //
                    # // In event of being no site whitelist, should we
                    #//  restrict the site whitelist to the list of sites
                    #  //containing the PU sample?
                    # // Ans.: Yes. /diego
                    #//
                    fileList, self.pileupSites = \
                        puDataset.getPileupFiles(*self.sites)
                    jobCfg.pileupFiles = fileList
                    logging.debug("Pileup Files Added: %s" % fileList)

        if not self.firstNodeCfg:
            self.firstNodeCfg = jobCfg
        jobSpecNode.cfgInterface = jobCfg
        return


    def generateCmsGenConfig(self, jobSpecNode):
        """
        _generateCmsGenConfig_

        Process CmsGen type nodes to insert maxEvents and run numbers
        for cmsGen jobs

        """
        if jobSpecNode.type != "CmsGen":
            return

        jobSpecNode.applicationControls['firstRun'] = self.count
        jobSpecNode.applicationControls['maxEvents'] = self.eventsPerJob
        jobSpecNode.applicationControls['randomSeed'] = randomSeed()
        jobSpecNode.applicationControls['fileName'] = "%s-%s.root" % (
            self.currentJob, jobSpecNode.name)
        jobSpecNode.applicationControls['logicalFileName'] = "%s-%s.root" % (
            self.currentJob, jobSpecNode.name)
        return




