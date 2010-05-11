#!/usr/bin/env python
"""
_WorkflowTools_


Common tools used in the creation of Workflow Specs

"""

# Silence deprecation warnings until all runtime python > python2.6
import warnings
warnings.filterwarnings("ignore", category = DeprecationWarning)
import popen2
warnings.filterwarnings("default", category = DeprecationWarning)
import sys
import time

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase
from ProdCommon.MCPayloads import UUID as MCPayloadsUUID
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvLoader import loadIMProvString

class NodeFinder:
    """
    Util to search a workflow for a node by name

    """
    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance


def createPSetHash(cfgFile):
    """
    _createPSetHash_

    Run the EdmConfigHash utility to create a PSet Hash for the
    cfg file provided.

    It will be written to cfgFile.hash, and also read back and returned
    by value.

    An Exception will be raised if the command fails

    """

    pop = popen2.Popen4("edmConfigHash  %s  " % cfgFile)
    pop.wait()
    exitStatus = pop.poll()
    if exitStatus:
        msg = "Error creating PSet Hash file:\n"
        msg += pop.fromchild.read()
        raise RuntimeError, msg

    content = pop.fromchild.read()
    return content.strip()


def createPythonConfig(cfgFile):
    """
    _createPythonConfig_

    Generate a Python Config from the cfgFile provided.
    Return the location of that file (Will be cfgFile.pycfg

    """
    pycfgFile = cfgFile.replace(".cfg", ".pycfg")
    pop = popen2.Popen4("EdmConfigToPython < %s > %s " % (cfgFile, pycfgFile))
    pop.wait()
    exitStatus = pop.poll()
    if exitStatus:
        msg = "Error creating Python cfg file:\n"
        msg += pop.fromchild.read()
        raise RuntimeError, msg

    #  //
    # // Check that python file is valid
    #//

    pop = popen2.Popen4("%s %s" % (sys.executable, pycfgFile))
    pop.wait()
    exitStatus = pop.poll()
    if exitStatus:
        msg = "Error importing Python cfg file:\n"
        msg += pop.fromchild.read()
        raise RuntimeError, msg

    return pycfgFile





def addStageOutNode(cmsRunNode, nodeName, *nodes):
    """
    _addStageOutNode_

    Given a cmsRun Node add a StageOut node to it with the name provided

    """
    if not nodes:
        nodes = [cmsRunNode.name]
    stageOut = cmsRunNode.newNode(nodeName)
    stageOut.type = "StageOut"
    stageOut.application["Project"] = ""
    stageOut.application["Version"] = ""
    stageOut.application["Architecture"] = ""
    stageOut.application["Executable"] = "RuntimeStageOut.py" # binary name


    config = IMProvNode("StageOutConfiguration")
    for node in nodes:
        config.addNode(IMProvNode("StageOutFor", None, NodeName = str(node)))

    config.addNode(IMProvNode("NumberOfRetries", None, Value = 3))
    config.addNode(IMProvNode("RetryPauseTime", None, Value = 600))



    stageOut.configuration = config.makeDOMElement().toprettyxml()
    return

def addLogArchNode(cmsRunNode, nodeName):
    """
    _addLogArchNode_

    Given a cmsRun Node add a LogArch node to it with the name provided

    """

    stageOut = cmsRunNode.newNode(nodeName)
    stageOut.type = "LogArchive"
    stageOut.application["Project"] = ""
    stageOut.application["Version"] = ""
    stageOut.application["Architecture"] = ""
    stageOut.application["Executable"] = "RuntimeLogArch.py" # binary name


    return

def addCleanUpNode(cmsRunNode, nodeName):
    """
    _addCleanUpNode_

    Add a clean up task following a cmsRun node. This will trigger a removal
    attempt on each of the inpiy files to the cmsRun Node.

    """
    cleanUp = cmsRunNode.newNode(nodeName)
    cleanUp.type = "CleanUp"
    cleanUp.application["Project"] = ""
    cleanUp.application["Version"] = ""
    cleanUp.application["Architecture"] = ""
    cleanUp.application["Executable"] = "RuntimeCleanUp.py" # binary name
    cleanUp.configuration = ""
    return


def addStageOutOverride(stageOutNode, command, option, seName, lfnPrefix):
    """
    _addStageOutOverride_

    Given the stageout node provided, add an Override to its configuration
    attribute

    """
    if len(stageOutNode.configuration.strip()) == 0:
        config = IMProvNode("StageOutConfiguration")
    else:
        config = loadIMProvString(stageOutNode.configuration)

    override = IMProvNode("Override")
    override.addNode(IMProvNode("command", command))
    override.addNode(IMProvNode("option" , option))
    override.addNode(IMProvNode("se-name" , seName))
    override.addNode(IMProvNode("lfn-prefix", lfnPrefix))
    config.addNode(override)
    stageOutNode.configuration = config.makeDOMElement().toprettyxml()
    return


def generateFilenames(workflowSpec):
    """
    _generateFilenames_

    Generate the LFN names for the workflowSpec instance provided

    """

    mergedLFNBase(workflowSpec)
    unmergedLFNBase(workflowSpec)
    return




def createProductionWorkflow(prodName, cmsswVersion, cfgFile = None,
                             category = "mc", **args):
    """
    _createProductionWorkflow_

    Create a Production style workflow, ie generation of new events

    """

    timestamp = int(time.time())
    if args.get("PyCfg", None) == None:
        if cfgFile == None:
            msg = "Error: No Cfg File or python cfg file provided to createProductionWorkflow"
            raise RuntimeError, msg
        pycfgFile = createPythonConfig(cfgFile)
        pycfgFileContent = file(pycfgFile).read()
    else:
        pycfgFileContent = args['PyCfg']



    if args.get("PSetHash", None) == None:
        realPSetHash = createPSetHash(cfgFile)
    else:
        realPSetHash = args['PSetHash']


    #  //
    # // Create a new WorkflowSpec and set its name
    #//
    spec = WorkflowSpec()
    workflowname = "%s__%s-%s-%s-%s"%(prodName,cmsswVersion,args.get("processingLabel","Test07"),args.get("physicsGroup","NoPhysicsGroup"),timestamp)
    spec.setWorkflowName(workflowname)
    spec.setRequestCategory(category)
    spec.setRequestTimestamp(timestamp)

    cmsRun = spec.payload
    populateCMSRunNode(cmsRun, "cmsRun1", cmsswVersion, pycfgFileContent, realPSetHash,
                       timestamp, prodName, physicsGroup = args.get("physicsGroup", "NoPhysicsGroup"), processingLabel=args.get("processingLabel", "Test07"), fakeHash = args.get("FakeHash", False))


    addStageOutNode(cmsRun, "stageOut1")
    generateFilenames(spec)
    return spec


def addPileupToSpec(spec, pileupDataset, filesPerJob, **options):
    """
    _addPileupToSpec_

    Add pileup info to the workflow spec.

    - pileupDataset is the /primary/tier/processed name of the PU dataset
    - filesPerJob is the number of pileup files per job to be used
    - options can contain:

      - NodeName - the name of the workflow node to attach the PU to,
        default is cmsRun1 as that is used by the tools in this module

      - DBS/DLS Contact Info for the DBS instance containing the PU dataset
        This defaults to the global DBS/DLS which should be the majority
        of cases.

    """

    dbsContacts = {}

    dbsContacts["DBSURL"] = options.get(
        "DBSURL",
        "http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery")
    dbsContacts["DBSAddress"] =  options.get("DBSAddress", "MCGlobal/Writer")
    dbsContacts["DBSType"] = options.get("DBSType", "CGI")
    dbsContacts["DLSAddress"] = options.get(
        "DLSAddress" , "prod-lfc-cms-central.cern.ch/grid/cms/DLS/LFC")
    dbsContacts["DLSType"] = options.get("DLSType" , "DLS_TYPE_DLI")

    nodeName = options.get("NodeName", "cmsRun1")

    finder = NodeFinder(nodeName)
    spec.payload.operate(finder)
    nodeInstance = finder.result
    if nodeInstance == None:
        msg = "Error adding pileup to Workflow Spec:\n"
        msg += "Couldnt find node in spec named: %s\n" % nodeName
        raise RuntimeError, msg



    puPrimary = pileupDataset.split("/")[1]
    puTier = pileupDataset.split("/")[2]
    puProc = pileupDataset.split("/")[3]
    puDataset = nodeInstance.addPileupDataset(puPrimary, puTier, puProc)
    puDataset['FilesPerJob'] = filesPerJob
    puDataset.update(dbsContacts)

    return


def createProcessingWorkflow(**args):
    """
    _createProcessingWorkflow_

    Create a Processing style workflow, ie consume a dataset.

    """
    pass


