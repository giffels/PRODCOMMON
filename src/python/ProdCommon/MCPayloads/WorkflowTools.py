#!/usr/bin/env python
"""
_WorkflowTools_


Common tools used in the creation of Workflow Specs

"""

import popen2
import sys
import time

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import unmergedLFNBase, mergedLFNBase
from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface
from ProdCommon.MCPayloads import UUID as MCPayloadsUUID
from IMProv.IMProvNode import IMProvNode



def createPSetHash(cfgFile):
    """
    _createPSetHash_

    Run the EdmConfigHash utility to create a PSet Hash for the
    cfg file provided.

    It will be written to cfgFile.hash, and also read back and returned
    by value.

    An Exception will be raised if the command fails

    """
    hashFile = "%s.hash" % cfgFile
    pop = popen2.Popen4("EdmConfigHash < %s > %s " % (cfgFile, hashFile))
    pop.wait()
    exitStatus = pop.poll()
    if exitStatus:
        msg = "Error creating PSet Hash file:\n"
        msg += pop.fromchild.read()
        raise RuntimeError, msg

    content = file(hashFile).read()
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




def populateCMSRunNode(payloadNode, nodeName, version, pyCfgFileContent, hashValue,
                       timestamp, prodName, fakeHash = False):
    """
    _populateCMSRunNode_

    Fill in the details for a standard cmsRun node based on the
    contents of the configuration
    """
    payloadNode.name = nodeName
    payloadNode.type = "CMSSW"   
    payloadNode.application["Project"] = "CMSSW" # project
    payloadNode.application["Version"] = version # version
    payloadNode.application["Architecture"] = "slc3_ia32_gcc323" # obsolete
    payloadNode.application["Executable"] = "cmsRun" # binary name
    payloadNode.configuration = pyCfgFileContent # Python PSet file
    
    cfgInt = CfgInterface(payloadNode.configuration, True)
    
    for outModName, val in cfgInt.outputModules.items():
        datasets = val.datasets()
        for outDataset in datasets:
            dataTier = outDataset['dataTier']

            processedDS = "%s-%s-%s-unmerged" % (
                payloadNode.application['Version'], outModName, timestamp)

            if outDataset.has_key("processedDataset"):
                processedDS = outDataset['processedDataset']

            primaryName = prodName
            if outDataset.has_key("primaryDataset"):
                primaryName = outDataset['primaryDataset']
        

            outDS = payloadNode.addOutputDataset(primaryName, 
                                            processedDS,
                                            outModName)
                                        
            outDS['DataTier'] = dataTier
            outDS["ApplicationName"] = payloadNode.application["Executable"]
            outDS["ApplicationProject"] = payloadNode.application["Project"]
            outDS["ApplicationVersion"] = payloadNode.application["Version"]
            outDS["ApplicationFamily"] = outModName
            if fakeHash:
                guid = MCPayloadsUUID.uuidgen()
                if guid == None:
                    guid = MCPayloadsUUID.uuid()
                hashValue = "hash=%s;guid=%s" % (hashValue, guid)
                outDS['PSetHash'] = hashValue
            else:
                outDS['PSetHash'] = hashValue

    return
    

            
def addStageOutNode(cmsRunNode, nodeName):
    """
    _addStageOutNode_

    Given a cmsRun Node add a StageOut node to it with the name provided

    """
    
    stageOut = cmsRunNode.newNode(nodeName)
    stageOut.type = "StageOut"
    stageOut.application["Project"] = ""
    stageOut.application["Version"] = ""
    stageOut.application["Architecture"] = ""
    stageOut.application["Executable"] = "RuntimeStageOut.py" # binary name
    stageOut.configuration = ""

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
    
    override = IMProvNode("Override")

    override.addNode(IMProvNode("command", command))
    override.addNode(IMProvNode("option" , option))
    override.addNode(IMProvNode("se-name" , seName))
    override.addNode(IMProvNode("lfn-prefix", lfnPrefix))
    stageOutNode.configuration = override.makeDOMElement().toprettyxml()
    return

def generateFilenames(workflowSpec):
    """
    _generateFilenames_

    Generate the LFN names for the workflowSpec instance provided

    """
    
    mergedLFNBase(workflowSpec)
    unmergedLFNBase(workflowSpec)
    return




def createProductionWorkflow(prodName, cmsswVersion, cfgFile = None, category = "mc", **args):
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
    spec.setWorkflowName(prodName)
    spec.setRequestCategory(category)
    spec.setRequestTimestamp(timestamp)

    cmsRun = spec.payload
    populateCMSRunNode(cmsRun, "cmsRun1", cmsswVersion, pycfgFileContent, realPSetHash,
                       timestamp, prodName, fakeHash = args.get("FakeHash", False))
    
    
    addStageOutNode(cmsRun, "stageOut1")
    generateFilenames(spec)
    return spec
    



def createProcessingWorkflow(**args):
    """
    _createProcessingWorkflow_

    Create a Processing style workflow, ie consume a dataset.

    """
    pass

    
