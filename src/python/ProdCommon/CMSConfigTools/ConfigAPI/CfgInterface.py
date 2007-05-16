#!/usr/bin/env python
"""
_CfgInterface_

Wrapper class for a cmsconfig object with interfaces to manipulate
the InputSource and output modules

"""

import copy

from ProdCommon.CMSConfigTools.ConfigAPI.InputSource import InputSource
from ProdCommon.CMSConfigTools.ConfigAPI.OutputModule import OutputModule
from ProdCommon.CMSConfigTools.ConfigAPI.MaxEvents import MaxEvents
import ProdCommon.CMSConfigTools.ConfigAPI.Utilities as Utilities
import FWCore.ParameterSet.Types as CfgTypes

class CfgInterface:
    """
    _CfgInterface_

    Wrapper object for a cms Configuration object instance.
    Generates an InputSource object and OutputModules from the
    cfg file.

    Provides a clone interface that returns a new copy of itself to
    assist in generating jobs from a Template cfg file


    """
    def __init__(self, cfgInstance):
        self.data = cfgInstance
        self.inputSource = InputSource(self.data.source)
        self.outputModules = {}
        for omodName in self.data.outputModules:
            self.outputModules[omodName] = OutputModule(
                omodName, getattr(self.data, omodName))
        if not self.data.psets.has_key('maxEvents'):
            self.data.maxEvents = CfgTypes.untracked(
                CfgTypes.PSet()
                )
            
        
        self.maxEvents = MaxEvents(self.data.maxEvents)
        
            
    def clone(self):
        """
        _clone_

        return a new instance of this object by copying it

        """
        return copy.deepcopy(self)

    
    def __str__(self):
        """string rep of self: give python format PSet"""
        return self.data.dumpConfig()


    def mixingModules(self):
        """
        _mixingModules_

        return refs to all mixing modules in the cfg
        
        """
        result = []
	return result

    def insertSeeds(self, *seeds):
        """
        _insertSeeds_

        Insert the list of seeds into the RandomNumber Service

        """
        seedList = list(seeds)
        if "RandomNumberGeneratorService" not in self.data.services.keys():
            return
        svc = self.data.services["RandomNumberGeneratorService"]

        srcSeedVec = getattr(svc, "sourceSeedVector", Utilities._CfgNoneType()).value()
        if srcSeedVec != None:
            numReq = len(srcSeedVec)
            seedsReq = seedList[0:numReq]
            seedList = seedList[numReq+1:]
            svc.sourceSeedVector = CfgTypes.untracked( CfgTypes.vuint32(seedsReq))
            
            

        else:
            svc.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(seedList.pop(0)))
        modSeeds = getattr(svc, "moduleSeeds", Utilities._CfgNoneType()).value()
        if modSeeds != None:
            for param in modSeeds.parameterNames_():
                setattr(modSeeds, param, CfgTypes.untracked(CfgTypes.uint32(seedList.pop(0))))
        return
    
    def configMetadata(self):
        """
        _configMetadata_

        Get a dictionary of the configuration metadata from this cfg
        file if present

        """
        result = {}
        if "configurationMetadata" not in  self.data.psets.keys():
            return result
        cfgMeta = self.data.psets['configurationMetadata']
        for pname in cfgMeta.parameterNames_():
            result[pname] = getattr(cfgMeta, pname).value()
        return result
            
    
    def seedCount(self):
        """
        _seedCount_

        Get the number of required Seeds

        """
        return Utilities.seedCount(self.data)
    
    def validateForProduction(self):
        """
        _validateForProduction_

        Perform tests to ensure that the cfg object
        contains all the necessary pieces for production.

        Use this method to validate a cfg at request time

        """
        Utilities.checkMessageLoggerSvc(self.data)
        
        Utilities.checkConfigMetadata(self.data)

        for outMod in self.outputModules.values():
            Utilities.checkOutputModule(outMod.data)
            
        return
        

    def validateForRuntime(self):
        """
        _validateForRuntime_

        Perform tests to ensure that this config is suitable for
        Runtime operation

        """
        pass
        
        
