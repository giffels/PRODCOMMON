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
import FWCore.ParameterSet.Modules as CfgModules

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
        if not self.data.source:
            msg = "The config file has no Source"
            raise RuntimeError, msg
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
        return self.data.dumpPython()

    def setConditionsTag(self, condTag):
        """
        _setConditionsTag-

        Set the conditions glbal tag

        """
        globalPSet = getattr(
            self.data, "GlobalTag", None)
        if globalPSet == None:
            return
        globalTag = getattr(globalPSet, "globaltag", None)
        if globalTag == None:
            globalPSet.globalTag = CfgTypes.string(condTag)
        else:
            globalPSet.globaltag = condTag
        return

    def getConditionsTag(self):
        """
        _getConditionsTag_

        Retrieve the conditions tag if set, else None

        """
        globalPSet = getattr(
            self.data, "GlobalTag", None)
        if globalPSet == None:
            return None
        return getattr(
            globalPSet, "globaltag", Utilities._CfgNoneType()).value()


    def findMixingModulesByType(self, mixType):
        """
        _findMixingModulesByType_

        return refs to MixingModules by Type if present.
        Otherwise return None.

        """
        result = []
        prodsAndFilters = {}
        prodsAndFilters.update(self.data.producers)
        prodsAndFilters.update(self.data.filters)
        for key, value in prodsAndFilters.items():
            if value.type_() == mixType:
                result.append(value)

        return result


    def mixingModules(self):
        """
        _mixingModules_

        return refs to MixingModules if present.
        Otherwise return None.

        """
        return self.findMixingModulesByType("MixingModule")


    def dataMixPileupFileList(self):
        """
        _datamixPileupFilesList_

        return a list of pileup files from all datamixing modules

        """
        result = set()
        for mixMod in self.findMixingModulesByType("DataMixingModule"):
            secSource = getattr(mixMod, "secsource", None)
            if secSource == None: continue
            fileList = getattr(secSource, "fileNames",
                               Utilities._CfgNoneType()).value()
            if fileList == None: continue
            for entry in fileList:
                result.add(entry)
        return list(result)


    def pileupFileList(self):
        """
        _pileupFilesList_

        return a list of pileup files from all mixing modules

        """
        result = set()
        for mixMod in self.mixingModules():
            secSource = getattr(mixMod, "secsource", None)
            if secSource == None: continue
            fileList = getattr(secSource, "fileNames",
                               Utilities._CfgNoneType()).value()
            if fileList == None: continue
            for entry in fileList:
                result.add(entry)
        return list(result)


    def insertPileupFiles(self, *fileList):
        """
        _insertPileupFiles_

        Insert the files provided into all mixing modules

        """
        for mixMod in self.mixingModules():
            print "Processing MixingModule: %s " % mixMod
            secSource = getattr(mixMod, "input", None)
            if secSource == None:
                secSource = getattr(mixMod, "secsource", None)
            if secSource == None:
                msg = "==============WARNING================\n"
                msg += "No Input PoolRASource found for mixing module:\n"
                msg += mixMod.dumpConfig()
                msg += "\nCannot add Pileup Files...\n"
                msg += "======================================\n"
                print msg
                continue
            oldfileList = getattr(secSource, "fileNames", None)
            if oldfileList == None:
                print "No existing file list"
                continue
            setattr(secSource, 'fileNames', CfgTypes.untracked(
                CfgTypes.vstring()))

            for fileName in fileList:
                secSource.fileNames.append(str(fileName))
                print "PileupFile: %s " % str(fileName)

        return


    def setPileupFilesForSource(self, sourceName, *fileList):
        """
        _setPileupFilesForSource

        Insert the files provided into all mixing modules
        for the secsource named sourceName

        """
        for mixMod in self.mixingModules():
            print "Processing MixingModule: %s " % mixMod
            secSource = getattr(mixMod, sourceName, None)
            if secSource == None:
                msg = "==============WARNING================\n"
                msg += "No Input PoolRASource found for mixing module:\n"
                msg += mixMod.dumpConfig()
                msg += "\n With secsource named %s\n" % sourceName
                msg += " Cannot add Pileup Files...\n"
                msg += "======================================\n"
                print msg
                continue
            oldfileList = getattr(secSource, "fileNames", None)
            if oldfileList == None:
                print "No existing file list in secsource %s" % sourceName
                continue
            setattr(secSource, 'fileNames', CfgTypes.untracked(
                CfgTypes.vstring()))

            for fileName in fileList:
                secSource.fileNames.append(str(fileName))
                print "Adding %s PileupFile: %s " % (sourceName, str(fileName))

        return


    def setPileupFilesForModule(self, sourceName, targetModule, *fileList):
        """
        _setPileupFilesForSource

        Insert the files provided into all target modules
        for the secsource named sourceName

        """
        for mixMod in self.findMixingModulesByType(targetModule):
            print "Processing %s: %s " % (targetModule, mixMod)
            secSource = getattr(mixMod, sourceName, None)
            if secSource == None:
                msg = "==============WARNING================\n"
                msg += "No Input PoolRASource found for mixing module:\n"
                msg += mixMod.dumpConfig()
                msg += "\n With secsource named %s\n" % sourceName
                msg += " Cannot add Pileup Files...\n"
                msg += "======================================\n"
                print msg
                continue
            oldfileList = getattr(secSource, "fileNames", None)
            if oldfileList == None:
                print "No existing file list in secsource %s" % sourceName
                continue
            setattr(secSource, 'fileNames', CfgTypes.untracked(
                CfgTypes.vstring()))

            for fileName in fileList:
                secSource.fileNames.append(str(fileName))
                print "Adding %s PileupFile: %s " % (sourceName, str(fileName))

        return


    def insertSeeds(self, *seeds):
        """
        _insertSeeds_

        Insert the list of seeds into the RandomNumber Service

        """
        seedList = list(seeds)
        if "RandomNumberGeneratorService" not in self.data.services.keys():
            return


        if self.hasOldSeeds():
            self.insertOldSeeds(*seeds)
            return

        #  //
        # // Use seed service utility to generate seeds on the fly
        #//
        svc = self.data.services["RandomNumberGeneratorService"]
        try:
            from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
        except ImportError:
            msg = "Unable to import RandomeNumberServiceHelper"
            print msg
            raise RuntimeError, msg
        randHelper = RandomNumberServiceHelper(svc)
        randHelper.populate()
        svc.saveFileName = CfgTypes.untracked(
            CfgTypes.string("RandomEngineState.log"))

        return


    def hasOldSeeds(self):
        """
        _hasOldSeeds_

        Check to see if old or new seed service format is used

        """
        svc = self.data.services["RandomNumberGeneratorService"]
        srcSeedVec = getattr(svc, "sourceSeedVector",
                             Utilities._CfgNoneType()).value()
        srcSeed = getattr(svc, "sourceSeed",
                          Utilities._CfgNoneType()).value()

        testSeed = (srcSeedVec != None) or (srcSeed != None)
        return testSeed


    def insertOldSeeds(self, *seeds):
        """
        _insertOldSeeds_

        Backwards compatibility methods

        """
        seedList = list(seeds)
        svc = self.data.services["RandomNumberGeneratorService"]
        #  //=====Old methods, keep for backwards compat.=======
        # //
        #//
        srcSeedVec = getattr(svc, "sourceSeedVector",
                             Utilities._CfgNoneType()).value()
        if srcSeedVec != None:
            numReq = len(srcSeedVec)
            seedsReq = seedList[0:numReq]
            seedList = seedList[numReq + 1:]
            svc.sourceSeedVector = CfgTypes.untracked(
                CfgTypes.vuint32(seedsReq))


        else:
            svc.sourceSeed = CfgTypes.untracked(
                CfgTypes.uint32(seedList.pop(0)))
        modSeeds = getattr(svc, "moduleSeeds",
                           Utilities._CfgNoneType()).value()
        if modSeeds != None:
            for param in modSeeds.parameterNames_():
                setattr(modSeeds, param,
                        CfgTypes.untracked(CfgTypes.uint32(seedList.pop(0))))
        #  //
        # //
        #//====End old stuff======================================
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


    def setTFileAdaptorConfig(self, config):
        """
        Give the TFileAdaptor the provided config options
        """
        adaptor = self.data.services.get('AdaptorConfig')
        if adaptor is None:
            self.data.add_(CfgModules.Service('AdaptorConfig'))

        for option in config:
            value = str(config[option])
            # so far only support boolean & string options
            if option in ('enable', 'stats'):
                thisType = CfgTypes.bool._valueFromString
            else:
                thisType = CfgTypes.string

            setattr(self.data.services['AdaptorConfig'], option,
                    CfgTypes.untracked(thisType(value)))


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


