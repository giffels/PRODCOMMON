#!/usr/bin/env python
"""
_TaskState_

Runtime interface for reading in the state of a Task by reading the
RunResDB.xml and FrameworkJobReport.xml files and providing an
API to access the contents of them.

The object is instantiated with a directory that contains the task.


"""

__version__ = "$Revision: 1.14 $"
__revision__ = "$Id: TaskState.py,v 1.14 2009/06/17 14:06:23 direyes Exp $"
__author__ = "evansde@fnal.gov"


import os
import popen2


from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from RunRes.RunResComponent import RunResComponent
from RunRes.RunResDBAccess import loadRunResDB

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.CatalogParser import readCatalog
from ProdCommon.FwkJobRep.SiteLocalConfig import loadSiteLocalConfig

from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetDetails
from ProdCommon.MCPayloads.MergeTools import getSizeBasedMergeDatasetsFromNode



lfnSearch = lambda fileInfo, lfn:  fileInfo.get("LFN", None) == lfn
dsSearch = lambda fileInfo, dataset: len([x for x in fileInfo.dataset if x.name() == dataset]) != 0


def getTaskState(taskName):
    """
    _getTaskState_

    Find a task with the name provided in a job and, if it exists,
    instantiate a TaskState object for that Task.

    This method uses the RunResDB to look up the task location within
    the job and instantiates a TaskState for that task

    If the task is not found, None is returned

    """
    runresdb = os.environ.get("RUNRESDB_URL", None)
    if runresdb == None:
        return None
    try:
        rrdb = loadRunResDB(runresdb)
    except:
        return None
    query = "/RunResDB/%s/Directory[text()]" % taskName
    result =  rrdb.query(query)
    if len(result) == 0:
        return None
    result = result[0]
    dirname = os.path.join(os.environ['PRODAGENT_JOB_DIR'], result)

    taskState = TaskState(dirname)
    taskState.loadJobReport()
    taskState.loadRunResDB()
    if os.environ.has_key("PRODAGENT_JOBSPEC"):
        taskState.loadJobSpecNode()
    return taskState


class TaskState:
    """
    _TaskState_

    API object for extracting information from a CMSSW Task from the
    components of that task including the RunResDB and FrameworkJobReport


    """
    def __init__(self, taskDirectory):
        self.dir = taskDirectory
        self.jobReport = os.path.join(self.dir, "FrameworkJobReport.xml")
        self.runresdb = os.path.join(self.dir, "RunResDB.xml")
        self.jobSpecNode = None
        self.jobSpec = None

        self.taskAttrs = {}
        self.taskAttrs.setdefault("Name", None)
        self.taskAttrs.setdefault("CfgFile", None)
        self.taskAttrs.setdefault("PyCfgFile", None)
        self.taskAttrs.setdefault("WorkflowSpecID", None)
        self.taskAttrs.setdefault("JobSpecID", None)
        self.taskAttrs.setdefault("JobType", None)
        self.taskAttrs.setdefault("DoSizeMerge", False)
        self.taskAttrs.setdefault("MinMergeFileSize", 2000000000)

        self._RunResDB = None
        self._JobReport = None
        self._CatalogEntries = None
        self._SiteConfig = None
        self.runresLoaded = False
        self.jobReportLoaded = False
        self.catalogsLoaded = False
        self.siteConfigLoaded = False
        self.jobSpecLoaded = False

    def taskName(self):
        """
        _taskName_

        get the task name attribute

        """
        return self.taskAttrs['Name']


    def loadRunResDB(self):
        """
        _loadRunResDB_

        If the RunResDB file exists, load it

        """
        if not os.path.exists(self.runresdb):
            return
        improvNode = loadIMProvFile(self.runresdb)
        self._RunResDB = RunResComponent()
        self._RunResDB.children = improvNode.children
        self.runresLoaded = True

        dbDict = self._RunResDB.toDictionary()
        self.taskAttrs['Name'] = dbDict.keys()[0]
        tName = self.taskAttrs['Name']

        self.taskAttrs['WorkflowSpecID'] = \
                 dbDict[tName]['WorkflowSpecID'][0]
        self.taskAttrs['JobSpecID'] = \
                 dbDict[tName]['JobSpecID'][0]
        self.taskAttrs['JobType'] = \
                      dbDict[tName]['JobType'][0]

        if not dbDict[tName].has_key('SizeBasedMerge'):
            return
        doSizeMerge = dbDict[tName]['SizeBasedMerge'].get("DoSizeMerge", [])
        if len(doSizeMerge) > 0:
            if str(doSizeMerge[0]).lower() == "true":
                self.taskAttrs["DoSizeMerge"] = True

        mergeSize = dbDict[tName]['SizeBasedMerge'].get("MinMergeFileSize", [])
        if len(mergeSize) > 0:
            size = int(mergeSize[0])
            self.taskAttrs["MinMergeFileSize"] = size


        return

    def loadJobSpecNode(self):
        """
        _loadJobSpecNode_

        Load the job spec file referenced by PRODAGENT_JOB_SPEC env var and extract the node
        from it with the name provided

        """
        if not os.environ.has_key("PRODAGENT_JOBSPEC"):
            print " No PRODAGENT_JOBSPEC set"
            return
        specFile = os.environ['PRODAGENT_JOBSPEC']
        if not os.path.exists(specFile):
            print "Job Spec File %s does not exist" % specFile
            return
        jobSpec = JobSpec()
        jobSpec.load(specFile)
        self.jobSpec = jobSpec
        self.jobSpecNode = jobSpec.findNode(self.taskAttrs['Name'])
        self.jobSpecLoaded = True
        return



    def configurationDict(self):
        """
        _configurationDict_

        Return the RunResDB for this task name as a dictionary

        """
        try:
            result = self._RunResDB.toDictionary()[self.taskName()]
        except StandardError, ex:
            result = {}
        return result




    def getExitStatus(self):
        """
        _getExitStatus_

        If the task dir contains a file named exit.status, it will be
        read and converted into an integer and returned

        If the file does not exist, or cannot be parsed into an integer,
        None will be returned

        """
        exitFile = os.path.join(self.dir, "exit.status")
        if not os.path.exists(exitFile):
            return None
        content = file(exitFile).read()
        content = content.strip()
        try:
            exitCode = int(content)
            return exitCode
        except:
            return None



    def loadJobReport(self):
        """
        _loadJobReport_

        Extract the JobReport from the job report file if it exists

        """
        if not os.path.exists(self.jobReport):
            return

        jobReport = readJobReport(self.jobReport)[0]
        self._JobReport = jobReport
        self.jobReportLoaded = True

        #  //
        # // Convert PFNs to absolute paths if they exist in this
        #//  directory
        for fileInfo in self._JobReport.files:
            pfn = fileInfo['PFN']
            if pfn.startswith("file:"):
                pfn = pfn.replace("file:", "")

            pfnPath = os.path.join(self.dir, pfn)
            if not os.path.exists(pfnPath):
                continue
            fileInfo['PFN'] = pfnPath
        return


    def dumpJobReport(self):
        """
        _dumpJobReport_

        Read the Job Report file and dump it to stdout

        """
        print "======================Dump Job Report======================"
        if os.path.exists(self.jobReport):
            handle = open(self.jobReport, 'r')
            content = handle.read()
            handle.close()
            print content
            backupCopy = os.path.join(self.dir,
                                      "FrameworkJobReport-Backup.xml")
            handle2 = open(backupCopy, 'w')
            handle2.write(content)
            handle2.close()

        else:
            print "NOT FOUND: %s" % self.jobReport
        print "======================End Dump Job Report======================"
        return

    def getJobReport(self):
        """
        _getJobReport_

        Return a reference to the FkwJobReport object so that it can be
        manipulated

        """
        return self._JobReport

    def saveJobReport(self):
        """
        _saveJobReport_

        After modifying the JobReport in memory, commit the changes back to
        the JobReport file

        """
        self._JobReport.write(os.path.join(self.dir,"FrameworkJobReport.xml"))
        return




    def loadSiteConfig(self):
        """
        _loadSiteConfig_

        Load the Site config into this state object

        """
        try:
            self._SiteConfig = loadSiteLocalConfig()
            self.siteConfigLoaded = True
        except StandardError, ex:
            msg = "Unable to load SiteLocalConfig:\n"
            msg += str(ex)
            print msg
            self._SiteConfig = None
        return

    def getSiteConfig(self):
        """
        _getSiteConfig_

        Return the SiteLocalConfig instance if available, None if
        isnt

        """
        if not self.siteConfigLoaded:
            self.loadSiteConfig()
        return self._SiteConfig



    def outputDatasets(self):
        """
        _outputDatasets_

        Retrieve a list of output datasets from the RunResDB

        """
        if not self.jobSpecLoaded:
            return []

        datasets = getOutputDatasetDetails(self.jobSpecNode)
        datasets.extend(getSizeBasedMergeDatasetsFromNode(self.jobSpecNode))
        outModules = self.jobSpecNode.cfgInterface.outputModules

        for dataset in datasets:
            modName = dataset.get('OutputModuleName', None)
            if outModules.has_key(modName):
                dataset['LFNBase'] = outModules[modName].get('LFNBase', None)
                dataset['MergedLFNBase'] = outModules[modName].get('MergedLFNBase', None)


        return datasets



    def inputSource(self):
        """
        _inputSource_

        Get a dictionary of information about the input source from
        the RunResDB

        """
        result = {}
        if not self.runresLoaded:
            return result
        dbDict = self._RunResDB.toDictionary()
        inputParams = dbDict[self.taskAttrs['Name']]['Input']
        for key, value in inputParams.items():
            if key == "InputFiles":
                inputFiles = value['InputFile']
                result['InputFiles'] = inputFiles
                continue
            if len(value) == 0:
                continue
            if len(value) == 1:
                result[key] = value[0]
            else:
                result[key] = value
        return result





    def assignFilesToDatasets(self):
        """
        _assignFilesToDatasets_

        Match each file in the job report with the parameters describing the
        dataset that it belongs to.

        Matching is done by matching OutputModuleName in dataset to
        ModuleLabel for the File entry

        """

        datasets = self.outputDatasets()
        datasetMap = {}
        for dataset in datasets:
            datasetMap[dataset['OutputModuleName']] = dataset

        for fileInfo in self._JobReport.files:
            self.matchDataset(fileInfo, datasetMap)

        for fileInfo in self._JobReport.files:
            self.updateFileLFN(fileInfo)


        # have to do this after the dataset/file matching is complete
        for fileInfo in self._JobReport.files:
            self.matchFileParents(fileInfo)
            
        for fileInfo in self._JobReport.files:
        	# Assume at least one outModule exists.
        	# Job spec node should be loaded at this point.
        	#TODO: If above assumption is not correct, add the safety check 
            outModules = self.jobSpecNode.cfgInterface.outputModules
            fileInfo["Stream"] = outModules.values()[0].get("Stream", None)

        return


    def matchDataset(self, fileInfo, datasetMap):
        """
        _matchDataset_

        Associate the file to a dataset, switching it to the merged dataset and LFN
        if the size is over the correct value

        """
        outModLabel = fileInfo.get("ModuleLabel", None)
        print "Output Module Label: %s" % outModLabel
        if outModLabel == None:
            return


        if self.taskAttrs['DoSizeMerge']:
            print "Doing Size Merge Check"
            unmergedDs = datasetMap.get(outModLabel, None)
            if unmergedDs is not None:
                if unmergedDs['MergedLFNBase'] != None:
                    if fileInfo['Size'] >= self.taskAttrs['MinMergeFileSize']:
                        #  //
                        # // File bigger than threshold
                        #//
                        mergeModLabel = "%s-Merged" % outModLabel
                        ds = datasetMap.get(mergeModLabel, None)

                        if ds != None:
                            outModLabel = mergeModLabel
                            fileInfo['MergedBySize'] = "True"
                            fileInfo['MergedLFNBase'] = unmergedDs['MergedLFNBase']
                            msg = "File Associated to Merge Output based on size:\n"
                            msg += " %s\n Size = %s\n" % (fileInfo['LFN'], fileInfo['Size'])
                            print msg
                    else:
                        print "File is smaller than %s" % self.taskAttrs['MinMergeFileSize']
                else:
                    print "No MergedLFNBase defined, skipping Size Merge Check"
            else:
                print "No file is staged out, skipping Size Merge Check"


        controlKeys = ["FixedLFN","DisableGUID"  ]
        if datasetMap.has_key(outModLabel):
            datasetMatch = datasetMap[outModLabel]
            datasetForFile = fileInfo.newDataset()
            datasetForFile.update(datasetMatch)
            for ck in controlKeys:
                if datasetMatch.has_key(ck):
                    fileInfo[ck] = datasetMatch[ck]


            msg = "File: %s\n" % fileInfo['LFN']
            msg += "Produced By Output Module: %s\n" % outModLabel
            msg += "Associated To Datasets:\n"
            for ds in fileInfo.dataset:
                msg += " ==> /%s/%s/%s\n" % (
                    ds['PrimaryDataset'],
                    ds['DataTier'],
                    ds['ProcessedDataset'],
                    )
            print msg
        return


    def updateFileLFN(self, fileInfo):
        """
        _updateFileLFN_

        Given the file information and the output module/dataset informtion to which it was matched,
        update the LFN based on the configuration in the output module

        Algorithm:

        Check OutputModule settings to see if the LFN is fixed by presence of FixedLFN setting.

        If not fixed, insert a GUID unless DisableGUID is set.

        If MergedBySize is True, adapt the LFN to the merged LFN base.

        """
        msg = "Updating LFN: %s\n" % fileInfo['LFN']
        print msg
        fixedLFN = fileInfo.get("FixedLFN", None)
        if str(fixedLFN).lower() == "true":
            fixedLFN = True
        else:
            fixedLFN = False


        msg = "FixedLFN Option is %s\n" % fixedLFN
        print msg
        if fixedLFN:
            # do nothing to LFN
            return

        disableGUID = fileInfo.get("DisableGUID", None)
        if str(disableGUID).lower() == "true":
            disableGUID = True
        else:
            disableGUID = False
        msg = "DisableGUID Option is %s\n" % disableGUID
        print msg

        # add guid to LFN
        if not disableGUID:
            if fileInfo['GUID'] != None:
                fileInfo['LFN'] = '%s/%s.root' % \
                                  (os.path.dirname(fileInfo['LFN']), fileInfo['GUID'])

            msg = "GUID Inserted for LFN: %s\n" % fileInfo['LFN']
            print msg

        # merged by size update
        if fileInfo['MergedBySize'] == "True":
            # the MergedLFNBase already has a / in it -- removing that first
            mergeprefix=fileInfo['MergedLFNBase'] 
            while mergeprefix.endswith("/"):
               mergeprefix=mergeprefix[:-1]         
            newLFN = "%s/%s" % ( mergeprefix,
                                 os.path.basename(fileInfo['LFN']))
            fileInfo['LFN'] = newLFN
            msg = "File is merged by size, updating LFN: %s\n" % fileInfo['LFN']
            print msg

        return






    def matchFileParents(self, fileInfo):
        """

        Set input files based on their membership of parent datasets, these
        will either be the original input files or in the case's where the
        parentage (actually parentDataset) is overriden (i.e. for HLTDEBUG)
        only those files belonging to that dataset will be used (irrespective
        of what actual input files were run over).

        This is used to fake the file/dataset parentage info as required by the
        2 file read.

        I.e. If RAW and HLTDEBUG are created in the same process this can be
        used to force HLTDEBUG to be a child of RAW.

        """
        parentageWiped = False

        for ds in fileInfo.dataset:
            parentDataset = ds.get('ParentDataset', None)

            if not parentDataset:
                continue

            parentFiles = [x for x in self._JobReport.files if \
                                                    dsSearch(x, parentDataset)]
            if not parentFiles:
                # Input files weren't produced in this job
                # Standard processing job - leave InputFiles alone
                continue

            if not parentageWiped:
                fileInfo.inputFiles = []
                parentageWiped = True

            for parFile in parentFiles:
                if parFile['LFN'] in [x['LFN'] for x in fileInfo.inputFiles]:
                    continue
                print "Add InputFile %s for %s" % (parFile['LFN'], fileInfo['LFN'])
                fileInfo.addInputFile(parFile['PFN'], parFile['LFN'])
        return


    def generateFileStats(self):
        """
        _generateFileStats_

        For each File in the job report, if the file exists, record its
        size and cksum value

        """

        for fileInfo in self._JobReport.files:
            pfn = fileInfo['PFN']
            if pfn.startswith("file:"):
                pfn = pfn.replace("file:", "")
            if not os.path.exists(pfn):
                continue
            size = os.stat(pfn)[6]
            fileInfo['Size'] = size
            fileInfo.addChecksum("cksum", readCksum(pfn))
        return

    def reportFiles(self):
        """
        _reportFiles_

        Return a list of FileInfo objects from the JobReport

        """
        result = []
        if not self.jobReportLoaded:
            return result

        return self._JobReport.files



def readCksum(filename):
    """
    _readCksum_

    Run a cksum command on a file an return the checksum value

    """
    pop = popen2.Popen4("cksum %s" % filename)
    while pop.poll() == -1:
        exitStatus = pop.poll()
    exitStatus = pop.poll()
    if exitStatus:
        return None
    content = pop.fromchild.read()
    value = content.strip()
    value = content.split()[0]
    return value


