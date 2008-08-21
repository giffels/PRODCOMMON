#!/usr/bin/env python
"""
_RunJobFactory_

Given a processing workflow, generate a complete set of
job specs for it splitting the dataset on a one Run per job
basis.

Note: This is expected to be used for DQM harvesting only at present


"""

import os
import logging


from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdCommon.DataMgmt.Pileup.PileupDataset import createPileupDatasets


from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.JobSplit.JobSplitter import JobDefinition


def listRunsInDataset(reader, datasetName):
    """
    _listRunsInDataset_


    """
    runList = reader.dbs.listRuns(datasetName)

    result = [ x['RunNumber'] for x in reader.dbs.listRuns(datasetName) ]

    return result

def listFilesInRun(reader, datasetName, runNumber):
    """
    _listFilesInRun_


    """

    fileList = reader.dbs.listFiles(
        path = datasetName,
        runNumber = runNumber)
    return [ x['LogicalFileName'] for x in fileList]



def splitDatasetByRun(datasetName, dbsUrl):
    """
    _splitDatasetByRun_

    Chop up a dataset into a set of jobs with 1 job per run

    """
    reader = DBSReader(dbsUrl)
    result = []
    for run in listRunsInDataset(reader, datasetName):
        files = listFilesInRun(reader, datasetName, run)
        job = JobDefinition()
        job['LFNS'] = files
        job['RunNumber'] = run
        result.append(job)
    return result






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





class RunJobFactory:
    """
    _JobFactory_

    Working from a processing Workflow template, generate
    a complete set of job spec files from it

    """
    def __init__(self, workflowSpec, workingDir, dbsUrl, **args):
        self.workingDir = workingDir
        self.useInputDataset = None
        self.workflowSpec = workflowSpec
        self.dbsUrl = dbsUrl
        self.count = 0

        siteName = args.get("SiteName", None)
        self.allowedSites = []
        if siteName != None:
            self.allowedSites.append(siteName)

        self.filterRuns = False
        self.allowedRuns = []
        if args.get("FilterRuns", None) != None:
            self.allowedRuns = args['FilterRuns']
            self.filterRuns = True



        self.currentJob = None
        self.currentJobDef = None

        if args.has_key("InputDataset"):
            self.useInputDataset = args['InputDataset']



        self.generators = GeneratorMaker()
        self.generators(self.workflowSpec.payload)





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
        result = []
        for jobDef in self.processDataset():
            self.count = jobDef['RunNumber']
            newJobSpec = self.createJobSpec(jobDef)
            jobDict = {
                "JobSpecId" : self.currentJob,
                "JobSpecFile": newJobSpec,
                "JobType" : "Processing",
                "WorkflowSpecId" : self.workflowSpec.workflowName(),
                "WorkflowPriority" : 10,
                "Sites" : jobDef['SENames'],
                "Run" : jobDef['RunNumber'],
                }
            result.append(jobDict)


        return result


    def overrideInputDataset(self, inputDataset):
        """
        _overrideInputDataset_

        Set the name of the dataset in the workflow spec instance
        and override it

        """
        self.useInputDataset = inputDataset
        return


    def processDataset(self):
        """
        _processDataset_

        Import the Dataset contents and create a set of jobs from it

        """

        #  //
        # // Now create the job definitions
        #//
        logging.debug("AllowedSites = %s" % self.allowedSites)

        jobDefs = splitDatasetByRun(self.inputDataset(),
                                    self.dbsUrl)
        [ x.__setitem__('SENames', self.allowedSites) for x in jobDefs]

        if self.filterRuns:
            logging.debug("Filter Runs = %s" % self.allowedRuns)
            jobDefs = [ x for x in jobDefs if x['RunNumber'] in self.allowedRuns ]
        return jobDefs




    def inputDataset(self):
        """
        _inputDataset_

        Extract the input Dataset from this workflow

        """
        if self.useInputDataset != None:
            return self.useInputDataset
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

        logging.debug("Creating Job Spec: %s" % jobName)
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
            'maxEvents' : -1,
            }


        jobCfg = generator(self.currentJob, **args)



        jobSpecNode.cfgInterface = jobCfg
        return





