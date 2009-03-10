#!/usr/bin/env python
"""
_CfgGenerator_

Object that takes a CMSSWConfig object and adds per job attributes
to it


"""
import copy
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.SeedService import randomSeed




class CfgGenerator:
    """
    _CfgGenerator_

    """
    def __init__(self, cmsswConfigData, isString = False, appControls = {} ):
        self.template = cmsswConfigData
        self.appControls = appControls
        if isString == True:
            self.template = CMSSWConfig()
            self.template.unpack(cmsswConfigData)



    def __call__(self, jobName, **args):
        """
        _operator()_

        Insert per job information into a copy of the template
        CMSSWConfig object and return it

        """

        newCfg = self.template.lightweightClone()


        #  //
        # // Output modules first, use the module name in the
        #//  parameters in case of multiple modules
        #  //
        # //
        #//
        for modName in newCfg.outputModules.keys():
            outModule = newCfg.getOutputModule(modName)
            outModule['catalog'] = "%s-%s-Output.xml" % (jobName, modName)
            outModule['fileName'] = "%s-%s.root" % (jobName, modName)
            outModule['logicalFileName'] = "%s-%s.root" % (jobName, modName)
            if outModule.has_key('LFNBase'):
                outModule['logicalFileName'] = "%s/%s" % (
                    outModule['LFNBase'], outModule['logicalFileName']
                    )


        maxEvents = args.get("maxEvents", None)
        if maxEvents != None:

            selectionEff = self.appControls.get("SelectionEfficiency", None)
            evMultiplier = self.appControls.get("EventMultiplier", None)

            #  //
            # // Adjust number of events for selection efficiency
            #//
            if selectionEff != None:
                newMaxEv = float(maxEvents) / float(selectionEff)
                maxEvents = int(newMaxEv)

            # // If this node has an Event Multiplier, adjust maxEvents
            #//
            if evMultiplier != None:
                maxEvents = int(maxEvents) * int(evMultiplier)

            newCfg.setInputMaxEvents(maxEvents)

        maxOutputEvents = args.get("maxEventsWritten", None)
        if maxOutputEvents != None:
            newCfg.setOutputMaxEvents(maxOutputEvents)



        skipEvents = args.get("skipEvents", None)
        if skipEvents != None:
            newCfg.sourceParams['skipEvents'] = skipEvents
        firstEvent = args.get("firstEvent", None)
        if firstEvent != None:
            newCfg.sourceParams['firstEvent'] = firstEvent

        firstRun = args.get("firstRun", None)
        if firstRun != None:
            newCfg.sourceParams['firstRun'] = firstRun

        firstLumi = args.get("firstLumi", None)
        if firstLumi != None:
            newCfg.sourceParams['firstLuminosityBlock'] = firstLumi

        fileNames = args.get("fileNames", None)
        if fileNames != None:
            #newCfg.inputFiles.extend(fileNames)
            newCfg.inputFiles = fileNames

        seeds = [ randomSeed() for i in range(0, newCfg.requiredSeeds+1)]
        newCfg.seeds = seeds


        return newCfg




