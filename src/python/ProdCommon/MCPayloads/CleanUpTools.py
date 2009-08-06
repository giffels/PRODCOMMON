#!/usr/bin/env python
"""
_CleanUpTools_

Utilities for generating cleanup jobs

"""

import time
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.UUID import makeUUID
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools



def createCleanupWorkflowSpec():
    """
    _createCleanupWorkflowSpec_

    Create a generic cleanup WorkflowSpec definition
    that can be used to generate a sanbox for cleanup jobs

    """
    timestamp = str(time.asctime(time.localtime(time.time())))
    timestamp = timestamp.replace(" ", "-")
    timestamp = timestamp.replace(":", "_")
    workflow = WorkflowSpec()
    workflow.setWorkflowName("CleanUp-%s" % timestamp)
    workflow.setActivity("CleanUp")
    workflow.setRequestCategory("mc-cleanup")
    workflow.setRequestTimestamp(timestamp)
    workflow.parameters['WorkflowType']="CleanUp"
    

    cleanUp = workflow.payload
    cleanUp.name = "cleanUp1"
    cleanUp.type = "CleanUp" 
    
    cleanUp.application["Project"] = ""
    cleanUp.application["Version"] = ""
    cleanUp.application["Architecture"] = ""
    cleanUp.application["Executable"] = "RuntimeCleanUp.py" # binary name
    cleanUp.configuration = ""
    cleanUp.cfgInterface = None


    return workflow


def createCleanupJobSpec(workflowSpec, site, *lfns):
    """
    _createCleanupJob_

    Create a Cleanup JobSpec definition, using the cleanup
    workflow template, site name and the list of LFNs to be
    removed

    """

    jobSpec = workflowSpec.createJobSpec()
    jobName = "%s-%s" % (workflowSpec.workflowName(), makeUUID())
    jobSpec.setJobName(jobName)
    jobSpec.setJobType("CleanUp") 
    
    jobSpec.addWhitelistSite(site)

    
    lfnList = ""
    for lfn in lfns:
        lfnList += "%s\n" % lfn

    jobSpec.payload.configuration = lfnList

    return jobSpec
