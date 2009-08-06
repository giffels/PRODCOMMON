#!/usr/bin/env python
"""
LogCollectorTools

Utilities for generating LogArchive jobs

"""

import time
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.UUID import makeUUID
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvLoader import IMProvHandler
from IMProv.IMProvQuery import IMProvQuery

def createLogCollectorWorkflowSpec(wf):
    """
    _createLogColectorWorkflowSpec_

    Create a generic LogArchive WorkflowSpec definition

    """
    timestamp = str(time.asctime(time.localtime(time.time())))
    timestamp = timestamp.replace(" ", "-")
    timestamp = timestamp.replace(":", "_")
    workflow = WorkflowSpec()
    workflow.setWorkflowName("LogCollect-%s" % timestamp)
    workflow.setActivity("LogCollect")
    workflow.setRequestCategory("logcollect")
    workflow.setRequestTimestamp(timestamp)
    workflow.parameters['WorkflowType']="LogCollect"
    

    logArchive = workflow.payload
    logArchive.name = "logCollect1"
    logArchive.type = "LogCollect" 
    #TODO: remove this?
    #logArchive.workflow = wf
    logArchive.configuration
    logArchive.application["Project"] = ""
    logArchive.application["Version"] = ""
    logArchive.application["Architecture"] = ""
    logArchive.application["Executable"] = "RuntimeLogCollector.py" # binary name
    logArchive.configuration = ""
    logArchive.cfgInterface = None
    
    #set stageOut override
    #cfg = IMProvNode("config")
    #stageOut = IMProvNode("StageOutParameters")
    #cfg.addNode()
    #WorkflowTools.addStageOutNode(logArchive, "StageOut1")
    #WorkflowTools.addStageOutOverride(logArchive, stageOutParams['command'],
    #                                  stageOutParams['option'],
    #                                  stageOutParams['se-name'],
    #                                  stageOutParams['lfnPrefix'])
                                      

    return workflow


def createLogCollectorJobSpec(workflowSpec, originalWf, site, lfnBase, stageOutParams, *lfns):
    """
    createLogCollectorJobSpec

    Create a LogArchive JobSpec definition, using the LogArchive
    workflow template, site name and the list of LFNs to be
    removed

    """

    jobSpec = workflowSpec.createJobSpec()
    jobName = "%s-%s" % (workflowSpec.workflowName(), makeUUID())
    jobSpec.setJobName(jobName)
    jobSpec.setJobType("LogCollect") 
    
    jobSpec.addWhitelistSite(site)

    confNode = IMProvNode("LogCollectorConfig")

    # add site and workflow to collect
    confNode.addNode(IMProvNode("wf", originalWf))
    confNode.addNode(IMProvNode("se", site))
    confNode.addNode(IMProvNode("lfnBase", lfnBase))
    

    # add logs to collect
    logNode = IMProvNode("LogsToCollect")
    for lfn in lfns:
        logNode.addNode(IMProvNode("lfn", lfn))
    
    confNode.addNode(logNode)
    
    # stageout
    if stageOutParams:
        stageOutNode = IMProvNode("Override")
    #    WorkflowTools.addStageOutOverride(confNode, stageOutParams['command'],
    #                                      stageOutParams['option'],
    #                                      stageOutParams['se-name'],
    #                                      stageOutParams['lfnPrefix'])
        
        
        stageOutNode.addNode(IMProvNode("command", stageOutParams['command']))
        stageOutNode.addNode(IMProvNode("option", stageOutParams['option']))
        stageOutNode.addNode(IMProvNode("se-name", stageOutParams['se-name']))
        stageOutNode.addNode(IMProvNode("lfn-prefix", stageOutParams['lfnPrefix']))
        confNode.addNode(stageOutNode)
    
    
    #jobSpec.payload.configuration = logNode.makeDOMElement().toprettyxml()
    jobSpec.payload.configuration = confNode.makeDOMElement().toprettyxml()

    return jobSpec
