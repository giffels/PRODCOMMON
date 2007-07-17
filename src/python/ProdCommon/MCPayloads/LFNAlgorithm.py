#!/usr/bin/env python
"""
_LFNAlgorithm_

Algorithmic generation of Logical File Names using the CMS LFN Convention

"""
__revision__ = "$Id: LFNAlgorithm.py,v 1.5 2007/07/06 15:00:57 hufnagel Exp $"
__version__ = "$Revision: 1.5 $"
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



class DefaultLFNMaker:
    """
    _DefaultLFNMaker_

    Generate and add in an LFN base setting for each output module
    that this operator acts on

    """
    def __init__(self, jobSpecInstance):
        self.jobSpecInstance = jobSpecInstance
        self.unmerged = True
        if self.jobSpecInstance.parameters['JobType'] == "Merge":
            self.unmerged = False
        self.jobname = jobSpecInstance.parameters['JobName']
        self.run = jobSpecInstance.parameters['RunNumber']
        self.lfnGroup = str(self.run // 1000).zfill(4)

    def __call__(self, node):
        """
        _operator(JobSpecNode)_

        Act on each JobSpec node
        """

        # should throw an error
        if node.cfgInterface == None:
            return

        #  //
        # // Extract LFN base from included WorkflowSpec parameters
        #//
        if self.unmerged:
            base = node.getParameter("UnmergedLFNBase")[0]
        else:
            base = node.getParameter("MergedLFNBase")[0]
            
        #  //
        # // iterate over outputmodules/data tiers
        #//  Generate LFN, PFN and Catalog for each module
        for modName, outModule in node.cfgInterface.outputModules.items():
            if ( not outModule.has_key('fileName') ):
                msg = "OutputModule %s does not contain a fileName entry" % modName
                raise RuntimeError, msg
            if outModule.has_key("LFNBase"):
                # already has a custom LFN base set
                # dont overwrite it
                continue
            
            outModule['LFNBase'] = os.path.join(base,
                                                outModule['dataTier'],
                                                str(self.lfnGroup))
            
        return



class JobSpecLFNMaker:
    """
    _JobSpecLFNMaker_

    Util class to traverse a JobSpec's nodes and generate and
    insert Unmerged LFNs for each node that requires them

    Create the LFN using:

    - *base* output of unmergedLFNBase method for workflow spec

    - *lfnGroup* integer counter used to partition lfn namespace
                  (derived from run number)

    - *dataTier* Data Tier of the file being produced

    - *fileName* file name as defined in the JobSpec

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

        # should throw an error
        if node.cfgInterface == None:
            return

        #  //
        # // Extract LFN base from included WorkflowSpec parameters
        #//
        base = node.getParameter("UnmergedLFNBase")[0]

        #  //
        # // iterate over outputmodules/data tiers
        #//  Generate LFN, PFN and Catalog for each module
        for modName, outModule in node.cfgInterface.outputModules.items():
            if ( not outModule.has_key('fileName') ):
                msg = "OutputModule %s does not contain a fileName entry" % modName
                raise RuntimeError, msg
            outModule['logicalFileName'] = os.path.join(base, outModule['dataTier'], str(self.lfnGroup))
            outModule['logicalFileName'] += '/'
            outModule['logicalFileName'] += outModule['fileName']

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
