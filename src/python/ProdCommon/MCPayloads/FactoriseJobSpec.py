#!/usr/bin/env python
"""
_FactoriseJobSpec_

Tool to take a JobSpec of N total events and split it into J jobs of
N/j events

"""

from ProdCommon.CMSConfigTools.CfgGenerator import CfgGenerator
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.LFNAlgorithm import createUnmergedLFNs

class CfgMaker(dict):
    """
    _CfgMaker_

    Operator to generate configuration files with proper event, run
    and job information when factorising a job spec
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("JobName", None)
        self.setdefault("RunNumber", None)
        self.setdefault("MaxEvents", None)
        self.setdefault("SkipEvents", None)
        self.update(args)

    def __call__(self, jobSpecNode):

        if jobSpecNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(jobSpecNode.configuration, True)
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return

        args = {}
        if self['MaxEvents'] != None:
            args['maxEvents'] = self['MaxEvents']
        if self['RunNumber'] != None:
            args['firstRun'] = self['RunNumber']
        if self['SkipEvents'] != None:
            args['skipEvents'] = self['SkipEvents']
        jobCfg = generator(self['JobName'], **args)

        jobSpecNode.configuration = jobCfg.cmsConfig.asPythonString()
        jobSpecNode.loadConfiguration()
        return

        
        


def factoriseJobSpec(jobSpecInstance, njobs, eventCount, **args):
    """
    _factoriseJobSpec_

    Method to split a JobSpec of N total events and split it into njobs
    jobs of N/njobs events

    The Run Number in the job spec instance is used as an initial run.

    TODO: <<<<NEEDS PILEUP DETAILS>>>>
    
    """
    
    runNumber = args.get("RunNumber",
                         int(jobSpecInstance.parameters['RunNumber']))
    firstEvent = args.get("FirstEvent", 0)
    maxRunNumber = args.get("MaxRunNumber", None)

    
    eventsPerJob = float(eventCount)/float(njobs)
    eventsPerJob = int(eventsPerJob)
    
    result = []

    workflowName = jobSpecInstance.payload.workflow

    template = jobSpecInstance.makeIMProv()
    
    currentRun = runNumber
    currentEvent = firstEvent
    
    for i in range(0, njobs):
        
        jobName = "%s-%s" % (workflowName, currentRun)
        newSpec = JobSpec()
        newSpec.loadFromNode(template)
        newSpec.setJobName(jobName)
        newSpec.parameters['RunNumber'] = currentRun
        
        generator = CfgMaker(JobName = jobName,
                             MaxEvents = eventsPerJob,
                             SkipEvents = currentEvent,
                             RunNumber = currentRun,
                             )

        newSpec.payload.operate(generator)
        createUnmergedLFNs(newSpec)

        result.append(newSpec)

        currentRun += 1
        currentEvent += eventsPerJob
        if maxRunNumber != None:
            if currentRun > maxRunNumber:
                break

    return result



