#!/usr/bin/env python
"""
_CMSSWConfig_

Object to allow manipulation & save/load of a cmsRun config file
without having to have the CMSSW based Python API.

This object can be instantiated when the API is present and populated,
then saved and manipulated without the API.
Then it can be used to generate the final cfg when the CMSSW API is
present at runtime.

All imports of the API are dynamic, this module should not depend
on the CMSSW API at top level since it makes it impossible to
use at the PA.

This object should be saved and added to PayloadNodes as the configuration
attribute

"""

import base64
import pickle


from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvString


class CMSSWConfig:
    """
    _CMSSWConfig_

    Serialisable buffer around a cfg file that can be used to
    store changes while the CMSSW API is not present and then
    insert them when it is.

    """
    def __init__(self):
        self.rawCfg = None
        #  //
        # // Source related parameters and seeds
        #//
        self.sourceParams = {}
        self.sourceType = None
        self.inputFiles = []
        self.requiredSeeds = 0
        self.seeds = []

        #  //
        # // Output controls
        #//
        self.outputModules = {}
        self.maxEvents = {
            'input' : None,
            'output' : None,
            }
        
        #  //
        # // Pileup/Mixing Module tinkering
        #//
        self.pileupFiles = []



    def setInputMaxEvents(self, maxEvents):
        """
        _setInputMaxEvents_

        Limit the number of events to be read, -1 is all

        """
        self.maxEvents["input"] = maxEvents
        return

    def setOutputMaxEvents(self, maxEvents, modName = None):
        """
        _setOutputMaxEvents_

        Set the limit on the maximum number of events to be written.
        Optionally one can provide per output module limits or a global
        limit.
        """
        if modName == None:
            self.maxEvents["output"] = maxEvents
        else:
            self.maxEvents[modName] = maxEvents
        return
    
        

    def getOutputModule(self, moduleName):
        """
        _addOutputModule_

        New Output module settings block

        """
        existingModule = self.outputModules.get(moduleName, None)
        if existingModule != None:
            return existingModule
        newModule = {}
        newModule.setdefault("Name", moduleName)
        newModule.setdefault("fileName", None)
        newModule.setdefault("logicalFileName", None)
        newModule.setdefault("catalog", None)
        newModule.setdefault("primaryDataset", None)
        newModule.setdefault("processedDataset", None)
        newModule.setdefault("dataTier", None)
        newModule.setdefault("filterName", None)
        self.outputModules[moduleName] = newModule
        return newModule


    def originalContent(self):
        """
        _originalContent_

        Return the original cfg file content

        """
        return ""

    def save(self):
        """
        _save_

        This instance to IMProv doc

        """
        result = IMProvNode("CMSSWConfig")

        #  //
        # // Save Source Info
        #//
        sourceNode = IMProvNode("Source")
        if len(self.inputFiles) > 0:
            sourceFiles = IMProvNode("InputFiles")
            [ sourceFiles.addNode(IMProvNode("File", str(x)))
               for x in self.inputFiles ]
            sourceNode.addNode(sourceFiles)
        for key, value in self.sourceParams.items():
            if value != None:
                sourceNode.addNode(
                    IMProvNode("Parameter", str(value), Name = str(key))
                    )
        if self.sourceType != None:
            sourceNode.addNode(
                IMProvNode("SourceType", None, Value = self.sourceType))
        result.addNode(sourceNode)
        
        seedNode = IMProvNode("Seeds")
        seedNode.addNode(
            IMProvNode("RequiredSeeds",
                       None, Value=str(self.requiredSeeds))
            )
        if len(self.seeds) > 0:
            [ seedNode.addNode(IMProvNode(
                "RandomSeed", None, Value=str(x))) for x in self.seeds ]
        
        result.addNode(seedNode)

        #  //
        # // Save Pileup settings
        #//
        pileupNode = IMProvNode("Pileup")
        if len(self.pileupFiles) > 0:
            pileupFiles = IMProvNode("PileupFiles")
            [ pileupFiles.addNode(IMProvNode("File", str(x)))
              for x in self.pileupFiles ]
            pileupNode.addNode(pileupFiles)
        result.addNode(pileupNode)

        #  //
        # // Save output data
        #//
        outNode = IMProvNode("Output")
        for outMod in self.outputModules.values():
            moduleNode = IMProvNode("OutputModule", None,
                                    Name = outMod['Name'])
            outNode.addNode(moduleNode)
            for key, val in outMod.items():
                if key == "Name":
                    continue
                if val == None:
                    continue
                moduleNode.addNode(IMProvNode(
                    key, str(val)
                    ))
        result.addNode(outNode)
        
        #  //
        # // Save maxEvents settings
        #//
        maxEvNode = IMProvNode("MaxEvents")
        
        [ maxEvNode.addNode(IMProvNode(x[0], None, Value = str(x[1])))
                            for x in self.maxEvents.items() if x[1] != None ]
            
        result.addNode(maxEvNode)

        #  //
        # // Save & Encode the raw configuration
        #//
        if self.rawCfg == None:
            data = ""
        else:
            data = base64.encodestring(self.rawCfg)
        configNode = IMProvNode("ConfigData", data, Encoding="base64")
        result.addNode(configNode)

        #origData = base64.encodestring(self.originalCfg)
        #origCfgNode = IMProvNode("OriginalCfg", origData, Encoding="base64")
        #result.addNode(origCfgNode)

        

        return result


    def load(self, improvNode):
        """
        _load_

        populate this instance from the node provided

        """
     
        srcFileQ = IMProvQuery("/CMSSWConfig/Source/InputFiles/File[text()]")
        srcParamQ = IMProvQuery("/CMSSWConfig/Source/Parameter")
        seedReqQ = IMProvQuery("/CMSSWConfig/Seeds/RequiredSeeds[attribute(\"Value\")]")
        seedValQ = IMProvQuery("/CMSSWConfig/Seeds/RandomSeed[attribute(\"Value\")]")

        #  //
        # // Source
        #//
        self.inputFiles = srcFileQ(improvNode)
        for srcParam in srcParamQ(improvNode):
            parName = srcParam.attrs.get('Name', None)
            if parName == None:
                continue
            parVal = str(srcParam.chardata)
            self.sourceParams[str(parName)] = parVal
        srcTypeQ = IMProvQuery(
            "/CMSSWConfig/Source/SourceType[attribute(\"Value\")]")
        srcTypeData = srcTypeQ(improvNode)
        if len(srcTypeData) > 0:
            self.sourceType = str(srcTypeData[-1])
        
        #  //
        # // seeds
        #//
        self.requiredSeeds = int(seedReqQ(improvNode)[0])
        seedVals = seedValQ(improvNode)
        [self.seeds.append(int(x)) for x in seedVals]

        #  //
        # // Pileup
        #//
        puFileQ = IMProvQuery("/CMSSWConfig/Pileup/PileupFiles/File[text()]")
        self.pileupFiles = puFileQ(improvNode)

        #  //
        # // maxEvents
        #//
        maxEvQ = IMProvQuery("/CMSSWConfig/MaxEvents/*")
        [ self.maxEvents.__setitem__(x.name, int(x.attrs['Value']))
          for x in maxEvQ(improvNode)]

        #  //
        # // Output Modules
        #//
        outModQ = IMProvQuery("/CMSSWConfig/Output/OutputModule")
        outMods = outModQ(improvNode)
        for outMod in outMods:
            modName = outMod.attrs['Name']
            newMod = self.getOutputModule(str(modName))
            for childNode in outMod.children:
                key = str(childNode.name)
                value = str(childNode.chardata)
                newMod[key] = value

        #  //
        # // data
        #//
        dataQ = IMProvQuery("/CMSSWConfig/ConfigData[text()]")
        data = dataQ(improvNode)[0]
        data = data.strip()
        if data == "":
            self.rawCfg = None
        else:
            self.rawCfg = base64.decodestring(data)

        #origQ = IMProvQuery("/CMSSWConfig/OriginalCfg[text()]")
        #origCfg = origQ(improvNode)[0]
        #origCfg = origCfg.strip()
        #if origCfg == "":
        #    self.originalCfg = ""
        #else:
        #    self.originalCfg = base64.decodestring(origCfg)
        return
    
    def pack(self):
        """
        _pack_

        Generate a string of self suitable for addition to a PayloadNode

        """
        return str(self.save())

    def unpack(self, strRep):
        """
        _unpack_

        Populate self with data from string representation

        """
        node = loadIMProvString(strRep)
        self.load(node)
        return
    


    def makeConfiguration(self):
        """
        _makeConfiguration_


        ***Uses CMSSW API***

        Given the pickled cfg file and parameters stored in this
        object, generate the actual cfg file

        """
        try:
            import FWCore.ParameterSet
        except ImportError, ex:
            msg = "Unable to import FWCore based tools\n"
            msg += "Only available with scram runtime environment:\n"
            msg += str(ex)
            raise RuntimeError, msg
        
        from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface

        cfgInstance = pickle.loads(self.rawCfg)
        cfg = CfgInterface(cfgInstance)

        #  //
        # //  Source params
        #//
        cfg.inputSource.setFileNames(*self.inputFiles)

        firstRun = self.sourceParams.get("firstRun", None)
        if firstRun != None:
            cfg.inputSource.setFirstRun(firstRun)
            
        skipEv = self.sourceParams.get("skipEvents", None)
        if skipEv != None:
            cfg.inputSource.setSkipEvents(skipEv)


        #  //
        # // maxEvents PSet
        #//
        for key, value in self.maxEvents.items():
            if key == "input":
                if value != None:
                    cfg.maxEvents.setMaxEventsInput(int(value))
            elif key == "output":
                if value != None:
                    cfg.maxEvents.setMaxEventsOutput(int(value))
            else:
                cfg.maxEvents.setMaxEventsOutput(int(value), key)

        #  //
        # // Random seeds
        #//
        seedslist = [ int(x) for x in self.seeds ]
        cfg.insertSeeds(*seedslist)

        return cfg.data
        
                
    def loadConfiguration(self, cfgInstance):
        """
        _loadConfiguration_

        ***Uses CMSSW API***

        Populate self by extracting information from the cfgInstance

        """
        try:
            import FWCore.ParameterSet
        except ImportError, ex:
            msg = "Unable to import FWCore based tools\n"
            msg += "Only available with scram runtime environment:\n"
            msg += str(ex)
            raise RuntimeError, msg
        
        from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
        self.rawCfg = pickle.dumps(cfgInstance)
        cfgInterface = CfgInterface(cfgInstance)

        #  //
        # // max Events and seeds data
        #//
        self.maxEvents.update(cfgInterface.maxEvents.parameters())
        self.requiredSeeds = cfgInterface.seedCount()

        #  //
        # // Source data
        #//
        sourceParams = cfgInterface.inputSource.sourceParameters()
        if sourceParams.has_key("fileNames"):
            self.inputFiles = sourceParams['fileNames']
            del sourceParams['fileNames']
        self.sourceParams.update(sourceParams)
        self.sourceType = cfgInterface.inputSource.sourceType
        #  //
        # // Output Module data
        #//
        for modName, outMod in cfgInterface.outputModules.items():
            newMod = self.getOutputModule(modName)
            modParams = outMod.moduleParameters()
            newMod.update(modParams)

        
        return cfgInterface
