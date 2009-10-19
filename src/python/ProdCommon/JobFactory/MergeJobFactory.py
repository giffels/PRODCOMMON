#!/usr/bin/env python
"""
_MergeJobFactory_

Given a processing workflow, generate a complete set of
job specs for it.


"""

__revision__ = "$Id: MergeJobFactory.py,v 1.8 2009/10/19 14:44:13 ewv Exp $"
__version__  = "$Revision: 1.8 $"
__author__   = "ewv@fnal.gov"


import os
import logging

from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.JobSplit.JobSplitter import JobDefinition
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.JobSplitting.SplitterFactory import SplitterFactory


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
            return      # Is not a config file
        try:
            generator = CfgGenerator(payloadNode.configuration, True,
                                         payloadNode.applicationControls)
            self[payloadNode.name] = generator
        except StandardError, ex:
            return      # Can't read config file => not a config file



class MergeJobFactory:
    """
    _MergeJobFactory_

    Working from a processing Workflow template, generate
    a complete set of job spec files from it which will merge
    input files into a larger output files

    """
    def __init__(self, workflowSpec, workingDir, dbsUrl, **args):
        self.workingDir = workingDir
        self.workflowSpec = workflowSpec
        self.dbsUrl = dbsUrl
        self.count = args.get("InitialRun", 0)

        self.currentJob    = None
        self.currentJobDef = None

        self.allowedSites = []

        self.mergeSize = 2*1024*1024*1024 # in GB

        self.generators = GeneratorMaker()
        self.generators(self.workflowSpec.payload)

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
            logging.info(msg)
            siteList = siteRestriction.split(",")
            for site in siteList:
                if len(site.strip() ) > 0:
                    self.allowedSites.append(site.strip())

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
            newJobSpec = self.createJobSpec(jobDef)
            jobDict = {
                "JobSpecId" : self.currentJob,
                "JobSpecFile": newJobSpec,
                "JobType" : "Merge",
                "WorkflowSpecId" : self.workflowSpec.workflowName(),
                "WorkflowPriority" : 10,
                "Sites" : jobDef['SENames'],
                }
            result.append(jobDict)
            self.count += 1

        return result


    def processDataset(self):
        """
        _processDataset_

        Import the Dataset contents and create a set of jobs from it

        """

        #  //
        # // Now create the job definitions
        #//
        logging.debug("MergeSize = %s" % self.mergeSize)
        logging.debug("AllowedSites = %s" % self.allowedSites)
        logging.debug("Connection to DBS at: %s" % self.dbsUrl)

        reader = DBSReader(self.dbsUrl)
        blockList = reader.dbs.listBlocks(dataset = self.inputDataset())
        jobDefs = []

        for block in blockList:
            blockName = block['Name']
            logging.debug("Getting files for block %s" % blockName)
            locations = reader.listFileBlockLocation(blockName)
            fileList  = reader.dbs.listFiles(blockName = blockName)
            if not fileList: # Skip empty blocks
                continue

            thefiles = Fileset(name='FilesToSplit')
            for f in fileList:
                f['Block']['StorageElementList'].extend(locations)
                wmbsFile = File(f['LogicalFileName'])
                [ wmbsFile['locations'].add(x) for x in locations ]
                wmbsFile['block'] = blockName
                wmbsFile['size']  = f['FileSize']
                thefiles.addFile(wmbsFile)

            work = Workflow()
            subs = Subscription(
                fileset = thefiles,
                workflow = work,
                split_algo = 'MergeBySize',
                type = "Merge")
            logging.debug("Info for Subscription %s" % subs)
            splitter = SplitterFactory()
            jobfactory = splitter(subs)

            jobGroups = jobfactory(
                merge_size=self.mergeSize,                # min in Bytes
                all_files=True                            # merge all files
                )
            if not jobGroups:
                raise(SyntaxError)
            for jobGroup in jobGroups:
                for job in jobGroup.getJobs():
                    jobDef = JobDefinition()
                    jobDef['LFNS'].extend(job.getFiles(type='lfn'))
                    jobDef['SkipEvents'] = 0
                    jobDef['MaxEvents'] = -1
                    [ jobDef['SENames'].extend(list(x['locations']))
                        for x in job.getFiles() ]
                    jobDefs.append(jobDef)

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
        jobName = "%s-%s" % (self.workflowSpec.workflowName(), self.count)

        logging.debug("Creating Job Spec: %s" % jobName)
        self.currentJob    = jobName
        self.currentJobDef = jobDef
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Merge")
        jobSpec.parameters['RunNumber'] = self.count

        jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(self.generateJobConfig)

        specCacheDir =  os.path.join(
            self.specCache, str(self.count // 1000).zfill(4))
        if not os.path.exists(specCacheDir):
            os.makedirs(specCacheDir)
        jobSpecFile = os.path.join(specCacheDir, "%s-JobSpec.xml" % jobName)

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

        args = {'fileNames' : self.currentJobDef['LFNS'], }

        args['maxEvents'] = -1

        jobCfg = generator(self.currentJob, **args)
        jobSpecNode.cfgInterface = jobCfg
        return
