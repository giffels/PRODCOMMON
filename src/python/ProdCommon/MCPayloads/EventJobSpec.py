#!/usr/bin/env python
"""
_CreateEventBasedJobSpec_

"""


from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator


def createJobSpec(jobSpecId,workflowSpecFile, filename, runNumber, eventCount,  firstEvent = None,saveString=False,loadString=True):

    #  //
    # // Load workflow
    #//
    workflowSpec = WorkflowSpec()
    if loadString:
        workflowSpec.loadString(workflowSpecFile)
    else:
        workflowSpec.load(workflowSpecFile)

    

    #  //
    # // Create JobSpec
    #//
    jobSpec = workflowSpec.createJobSpec()
    jobName = "%s-%s" % (
        workflowSpec.workflowName(),
        runNumber
            )


    #jobSpec.setJobName(jobName)
    jobSpec.setJobName(jobSpecId)
    jobSpec.setJobType("Processing")
    jobSpec.parameters['RunNumber'] = runNumber
    jobSpec.parameters['EventCount'] = eventCount

    jobSpec.payload.operate(DefaultLFNMaker(jobSpec))

    if firstEvent != None:
        jobSpec.parameters['FirstEvent'] = firstEvent

    cfgMaker = ConfigGenerator(jobSpec)
    jobSpec.payload.operate(cfgMaker)

    if saveString:    
       return jobSpec.saveString()
    jobSpec.save(filename)
    return


class ConfigGenerator:
    """
    _ConfigGenerator_

    Functor to operate on a JobSpecNode tree

    """
    def __init__(self, parentJobSpec):
        self.jobSpec = parentJobSpec


    def __call__(self, jobSpecNode):
        """
        _generateJobConfig_
        
        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File
        
        """
        if jobSpecNode.type != "CMSSW":
           return 

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
        jobCfg = generator(jobSpecNode.jobName,
                           maxEvents = self.jobSpec.parameters['EventCount'],
                           firstRun =  self.jobSpec.parameters['RunNumber'],
                           skipEvents = self.jobSpec.parameters.get("FirstEvent", None))
        jobSpecNode.configuration = jobCfg.pack()
        jobSpecNode.loadConfiguration()
        
        return


if __name__== '__main__':
    wf = "/home/evansde/work/PRODAGENT/work/RequestInjector/WorkflowCache/RelVal120pre310MuonsPt10-evansde-TEST1-Workflow.xml"
    createJobSpec(wf, "/tmp/TestJobSpec.xml", 10000, 100, 200)
