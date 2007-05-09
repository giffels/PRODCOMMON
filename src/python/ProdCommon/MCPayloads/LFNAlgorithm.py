#!/usr/bin/env python
"""
_LFNAlgorithm_

Algorithmic generation of Logical File Names using the CMS LFN Convention

"""
__revision__ = "$Id: LFNAlgorithm.py,v 1.3 2006/12/04 13:29:31 evansde Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "evansde@fnal.gov"

import time
import os

#  //
# // All LFNS start with this constant value
#//
_LFNBase = "/store"


def makeTimestampString(timestamp):
    """
    _makeTimestampString_

    Working from an integer timestamp, generate the datestamp fragment
    of the LFN.

    This uses gmtime as a convention so that all LFNs get the same
    timestamp based on the request, rather than the local time zone
    interpretation.

    Why GMT?  Because Im British dammit! God Save The Queen!

    """
    gmTuple = time.gmtime(timestamp)
    year = gmTuple[0]
    month = gmTuple[1]
    day = gmTuple[2]

    return "%s/%s/%s" % (year, month, day)



def unmergedLFNBase(workflowSpecInstance):
    """
    _unmergedLFNBase_

    Generate the base name for LFNs for the WorkflowSpec Instance
    provided

    """
    category = workflowSpecInstance.requestCategory()
    result = os.path.join(_LFNBase, "unmerged", category)
    timestamp = workflowSpecInstance.requestTimestamp()
    result = os.path.join(
        result,
        makeTimestampString(timestamp),      # time/date
        workflowSpecInstance.workflowName()  # name of workflow/request
        )
    #  //
    # // Add this to the WorkflowSpec instance
    #//
    workflowSpecInstance.parameters['UnmergedLFNBase'] = result
    return result

def mergedLFNBase(workflowSpecInstance, lfnGroup = None):
    """
    _mergedLFNBase_

    Generate the base name for LFNs for the WorkflowSpec Instance provided
    for the output of merge jobs.

    """
    category = workflowSpecInstance.requestCategory()
    result = os.path.join(_LFNBase, category)
    timestamp = workflowSpecInstance.requestTimestamp()
    result = os.path.join(
        result,
        makeTimestampString(timestamp),      # time/date
        workflowSpecInstance.workflowName()  # name of workflow/request
        )
    if lfnGroup != None:
        result += "/%s" % lfnGroup
    
    #  //
    # // Add this to the WorkflowSpec instance
    #//
    workflowSpecInstance.parameters['MergedLFNBase'] = result
    return result
    

def generateLFN(requestBase, lfnGroup, jobName, dataTier):
    """
    _generateLFN_

    Create the LFN using:

    - *requestBase* output of merged or unmergedLFNBase method for
                    workflow spec

    - *lfnGroup* integer counter used to partition lfn namespace

    - *jobName* The JobSpec ID

    - *dataTier* The Data Tier of the file being produced
                 (usually same as output module name)

    Note that this is a temporary LFN: When stage out is performed,
    the GUID of the file will be used as the basename instead. But we
    dont know the GUID until after the file has been created.

    """
    result = os.path.join(requestBase, dataTier, str(lfnGroup))
    result += "/"
    result += jobName
    result += "-"
    result += dataTier
    result += ".root"
    return result


class JobSpecLFNMaker:
    """
    _JobSpecLFNMaker_

    Util class to traverse a JobSpec's nodes and generate and
    insert Unmerged LFNs for each node that requires them

    We derive the lfn group value from the run number

    """
    def __init__(self, jobName, runNumber):
        self.jobName = jobName
        self.run = runNumber
        self.lfnGroup = str(runNumber // 1000).zfill(4)

    def __call__(self, node):
        """
        _operator(JobSpecNode)_

        Act on each JobSpec node
        """
        #  //
        # // Extract LFN base from included WorkflowSpec parameters
        #//
        base = node.getParameter("UnmergedLFNBase")[0]
        #  //
        # // iterate over outputmodules/data tiers
        #//  Generate LFN, PFN and Catalog for each module
        if node.cfgInterface == None:
            return
        for modName, outModule in node.cfgInterface.outputModules.items():
            dataTier = outModule['dataTier']
            lfn = generateLFN(base, self.lfnGroup, self.jobName, dataTier)
            outModule['fileName']  = os.path.basename(lfn)
            outModule['logicalFileName'] = lfn
        return

def createUnmergedLFNs(jobSpecInstance):
    """
    _createUnmergedLFNs_

    Generate and insert the Unmerged LFNs for the JobSpec instance
    provided

    """
    lfnMaker = JobSpecLFNMaker(jobSpecInstance.parameters['JobName'],
                               jobSpecInstance.parameters['RunNumber'])

    jobSpecInstance.payload.operate(lfnMaker)

    return
