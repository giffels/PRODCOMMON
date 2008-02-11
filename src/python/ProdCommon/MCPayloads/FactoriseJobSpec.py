#!/usr/bin/env python
"""
_FactoriseJobSpec_

Tool to take a JobSpec of N total events and split it into J jobs of
N/j events

"""
import math

from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker

from ProdCommon.CMSConfigTools.SeedService import randomSeed

class CfgMaker(dict):
    """
    _CfgMaker_

    Operator to generate configuration files with proper event, run
    and job information when factorising a job spec
    
    """
    def __init__(self, generators, **args):
        dict.__init__(self)
        self.generators = generators
        self.setdefault("JobName", None)
        self.setdefault("RunNumber", None)
        self.setdefault("MaxEvents", None)
        self.setdefault("SkipEvents", None)
        self.update(args)

    def __call__(self, jobSpecNode):

        if jobSpecNode.name not in self.generators.keys():
            return
        generator = self.generators[jobSpecNode.name]

        args = {}
        if self['MaxEvents'] != None:
            args['maxEvents'] = self['MaxEvents']
        if self['RunNumber'] != None:
            args['firstRun'] = self['RunNumber']
        if self['SkipEvents'] != None:
            args['skipEvents'] = self['SkipEvents']
        jobCfg = generator(self['JobName'], **args)

        jobSpecNode.cfgInterface = jobCfg
        
        return

    def generateCmsGenConfig(self,jobSpecNode):
        """
        _generateCmsGenConfig_
                                                                                                                      
        Process CmsGen type nodes to insert maxEvents and run numbers
        for cmsGen jobs
                                                                                                                      
        """
        if jobSpecNode.type != "CmsGen":
            return
        if self['RunNumber'] != None:
           jobSpecNode.applicationControls['firstRun'] = self['RunNumber']
        if self['MaxEvents'] != None: 
           jobSpecNode.applicationControls['maxEvents'] = self['MaxEvents']
        jobSpecNode.applicationControls['randomSeed'] = randomSeed()
        jobSpecNode.applicationControls['fileName'] = "%s-%s.root" % (
            jobSpecNode.jobName, jobSpecNode.name)
        jobSpecNode.applicationControls['logicalFileName'] = "%s-%s.root" % (
            jobSpecNode.jobName, jobSpecNode.name)
        return
        
        

class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance

    """
    def __init__(self):
        dict.__init__(self)


    def __call__(self, payloadNode):
        if payloadNode.type != "CMSSW":
            return
        
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






def factoriseJobSpec(jobSpecInstance, jobSpecDir,njobs=[], eventCount=0, **args):
    """
    _factoriseJobSpec_

    njobs is an array of globally unique run numbers


    TODO: <<<<NEEDS PILEUP DETAILS>>>>
    
    """
    generators = GeneratorMaker()
    jobSpecInstance.payload.operate(generators)
    #AFgenerators(jobSpecInstance.payload)

    runNumber = int(args.get("RunNumber",
                         int(jobSpecInstance.parameters['RunNumber'])))
    firstEvent = int(args.get("FirstEvent",0))
    maxRunNumber = args.get("MaxRunNumber", None)

    
    eventsPerJob = int(math.ceil(float(eventCount)/float(len(njobs))))
    
    result = []

    workflowName = jobSpecInstance.payload.workflow

    template = jobSpecInstance.makeIMProv()
    
    currentRun = runNumber
    currentEvent = firstEvent
    
    for run_number in njobs:
        #jobName = "%s-%s" % (workflowName, run_number)
        jobName = jobSpecInstance.parameters['JobName']+'_jobcut-'+workflowName+'-'+str(run_number)
        newSpec = JobSpec()
        newSpec.loadFromNode(template)
        newSpec.setJobName(jobName)
        newSpec.parameters['RunNumber'] = run_number

        newSpec.payload.operate(DefaultLFNMaker(newSpec))
        maker = CfgMaker(generators, JobName = jobName,
                         RunNumber = run_number,
                         MaxEvents = eventsPerJob,
                         SkipEvents = currentEvent)
        newSpec.payload.operate(maker)
        newSpec.payload.operate(maker.generateCmsGenConfig)

        
        newSpec.parameters['FirstEvent']=currentEvent
        newSpec.parameters['RunNumber']=run_number
        newSpec.parameters['EventCount']=eventsPerJob

        jobSpecLocation=jobSpecDir+'/'+newSpec.parameters['JobName']+'.xml'
        newSpec.save(jobSpecLocation)

        result.append({'id':newSpec.parameters['JobName'],'spec':jobSpecLocation,'events':eventsPerJob})

        currentRun += 1
        currentEvent += eventsPerJob
        if((eventsPerJob+currentEvent)>(firstEvent+int(eventCount))):
            eventsPerJob=firstEvent+int(eventCount)-currentEvent
        if maxRunNumber != None:
            if currentRun > maxRunNumber:
                break

    return result



