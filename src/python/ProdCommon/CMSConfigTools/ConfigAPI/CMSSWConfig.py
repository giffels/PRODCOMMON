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
import zlib
import pickle


from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvString


class CMSSWConfigExtension:
    """
    _CMSSWConfigExtension_

    Wrapper class to provide pickle/unpickle of extension objects

    """
    def __init__(self, name, data = None):
        self.data = data
        self.pickledData = None
        self.name = name
        self.unpickled = True

    def save(self):
        """
        _save_

        Pack the data object into an IMProvNode

        """
        pickleData = pickle.dumps(self.data)
        zipData = zlib.compress(pickleData)
        packedData = base64.encodestring(zipData)
        result = IMProvNode("Extension", packedData, Name = self.name)
        return result


    def load(self, improvNode):
        """
        _load_

        Unpack the pickled data object from the IMProvNode.
        Will keep the raw pickled data if there is a problem

        """
        self.unpickled = False
        decodeData = base64.decodestring(improvNode.chardata)
        unzipData = zlib.decompress(decodeData)
        try:
            self.data = pickle.loads(unzipData)
            self.unpickled = False
        except Exception, ex:
            msg = "Error unpickling data member"
            print msg
            self.pickledData = unzipData
        return






class CMSSWConfig:
    """
    _CMSSWConfig_

    Serialisable buffer around a cfg file that can be used to
    store changes while the CMSSW API is not present and then
    insert them when it is.

    """
    def __init__(self):
        self.rawCfg = None
        self.originalCfg = None
        #  //
        # // Source related parameters and seeds
        #//
        self.sourceParams = {}
        self.sourceType = None
        self.inputFiles = []
        self.inputOverrideCatalog = None
        self.requiredSeeds = 0
        self.seeds = []
        self.tFileAdaptorConfig = {}

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
        self.beamHaloPlusFiles = []
        self.beamHaloMinusFiles = []
        self.cosmicPileupFiles = []
        self.dataMixerFiles = []

        #  //
        # // conditions tag
        #//
        self.conditionsTag = None
        #process.GlobalTag.globaltag = 'IDEAL_V5::All'

        #  //
        # // cfg metadata used for creating datasets
        #//
        self.configMetadata = {}

        #  //
        # // Rather than building in lots of extra attrs for
        #//  adding more information, provide a generic way to
        #  //include blocks of information such as run, lumi
        # // etc as extension objects.
        #//  Objects added to this will be pickled and zipped to pack
        #  //them in an XML node.
        # // Keep these things small, or performance will start to stink
        #//
        self.extensions = {}

    def lightweightClone(self):
        result = CMSSWConfig()
        for key, value in self.__dict__.items():
            if key in ("originalCfg", "rawCfg"): continue
            setattr(result , key, value)
        return result

    def addExtension(self, extName, dataObject):
        """
        _addExtension_

        Add a data structure that will be stored as part of this
        object in the job spec

        dataObject must be pickleable.

        """
        newExt = CMSSWConfigExtension(extName, dataObject)
        self.extensions[extName] = newExt
        return



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
        return self.originalCfg

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
            sourceNode.addNode(IMProvNode("OverrideCatalog", self.inputOverrideCatalog))
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
                       None, Value = str(self.requiredSeeds))
            )
        if len(self.seeds) > 0:
            [ seedNode.addNode(IMProvNode(
                "RandomSeed", None, Value = str(x))) for x in self.seeds ]

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
        if len(self.beamHaloPlusFiles) > 0:
            bhPlusFiles = IMProvNode("BeamHaloPlusFiles")
            [ bhPlusFiles.addNode(IMProvNode("File", str(x)))
              for x in self.beamHaloPlusFiles ]
            pileupNode.addNode(bhPlusFiles)
        if len(self.beamHaloMinusFiles) > 0:
            bhMinusFiles = IMProvNode("BeamHaloMinusFiles")
            [ bhMinusFiles.addNode(IMProvNode("File", str(x)))
              for x in self.beamHaloMinusFiles ]
            pileupNode.addNode(bhMinusFiles)

        if len(self.cosmicPileupFiles) > 0:
            cosmicFiles = IMProvNode("CosmicPileupFiles")
            [ cosmicFiles.addNode(IMProvNode("File", str(x)))
              for x in self.cosmicPileupFiles ]
            pileupNode.addNode(cosmicFiles)
        if len(self.dataMixerFiles) > 0:
            dataMixFiles = IMProvNode("DataMixerPileupFiles")
            [ dataMixFiles.addNode(IMProvNode("File", str(x)))
              for x in self.dataMixerFiles ]
            pileupNode.addNode(dataMixFiles)



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
        # // Save conditions tag
        #//
        if self.conditionsTag == None:
            condNode = IMProvNode("ConditionsTag")
        else:
            condNode = IMProvNode("ConditionsTag", str(self.conditionsTag))
        result.addNode(condNode)

        #  //
        # // Save config metadata
        #//
        cfgMetaNode = IMProvNode("ConfigMetadata")

        [ cfgMetaNode.addNode(
            IMProvNode(x[0], None, Value = str(x[1])))
          for x in self.configMetadata.items() if x[1] != None ]

        result.addNode(cfgMetaNode)

        #  //
        # // Save & Encode the raw configuration
        #//
        if self.rawCfg == None:
            data = ""
        else:
            zipData = zlib.compress(self.rawCfg)
            data = base64.encodestring(zipData)

        configNode = IMProvNode("ConfigData", data, Encoding = "base64")
        result.addNode(configNode)

        if self.originalCfg != None:
            origData = base64.encodestring(self.originalCfg)
            origCfgNode = IMProvNode("OriginalCfg",
                                     origData, Encoding = "base64")
            result.addNode(origCfgNode)

        #  //
        # // Extensions
        #//
        extNode = IMProvNode("Extensions")
        for extName, extInstance in self.extensions.items():
            extNode.addNode(extInstance.save())
        result.addNode(extNode)

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
        overrideCatalogQ = IMProvQuery("/CMSSWConfig/Source/OverrideCatalog[text()]")

        #  //
        # // Source
        #//
        inputOverrideCatalogs = overrideCatalogQ(improvNode)
        if len(inputOverrideCatalogs) > 0:
            self.overrideInputCatalog = inputOverrideCatalogs[-1]
        else:
            self.overrideInputCatalog = None

        #  //
        # // input files
        #//
        inpFiles = srcFileQ(improvNode)
        for inpFile in inpFiles:
            self.inputFiles.append(str(inpFile))

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

        bhPlusFileQ = IMProvQuery(
            "/CMSSWConfig/Pileup/BeamHaloPlusFiles/File[text()]")
        self.beamHaloPlusFiles = bhPlusFileQ(improvNode)

        bhMinusFileQ = IMProvQuery(
            "/CMSSWConfig/Pileup/BeamHaloMinusFiles/File[text()]")
        self.beamHaloMinusFiles = bhMinusFileQ(improvNode)

        cosmicFileQ = IMProvQuery(
            "/CMSSWConfig/Pileup/CosmicPileupFiles/File[text()]")
        self.cosmicPileupFiles = cosmicFileQ(improvNode)

        dataMixFileQ = IMProvQuery(
            "/CMSSWConfig/Pileup/DataMixerPileupFiles/File[text()]")
        self.dataMixerFiles = dataMixFileQ(improvNode)

        #  //
        # // conditions tag
        #//
        condQ = IMProvQuery("/CMSSWConfig/ConditionsTag[text()]")
        condTags = condQ(improvNode)
        if len(condTags) == 0:
            self.conditionsTag = None
        else:
            condTag = condTags[-1]
            if len(condTag.strip()) == 0:
                condTag = None
            self.conditionsTag = condTag

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
        # // cfg metadata
        #//
        cfgMetaQ = IMProvQuery("/CMSSWConfig/ConfigMetadata/*")
        [ self.configMetadata.__setitem__(str(x.name), str(x.attrs['Value']))
          for x in cfgMetaQ(improvNode)]




        #  //
        # // data
        #//
        dataQ = IMProvQuery("/CMSSWConfig/ConfigData[text()]")
        data = dataQ(improvNode)[0]
        data = data.strip()
        if data == "":
            self.rawCfg = None
        else:
            decodeData = base64.decodestring(data)
            self.rawCfg = zlib.decompress(decodeData)

        origQ = IMProvQuery("/CMSSWConfig/OriginalCfg[text()]")
        origR = origQ(improvNode)
        if len(origR) > 0:
            origCfg = origQ(improvNode)[0]
            origCfg = origCfg.strip()
            self.originalCfg = base64.decodestring(origCfg)
        else:
            self.originalCfg = None

        #  //
        # // Extensions
        #//
        extQ = IMProvQuery("/CMSSWConfig/Extensions/Extension")
        extNodes = extQ(improvNode)
        for extNode in extNodes:
            nodeName = extNode.attrs.get("Name", None)
            if nodeName == None: continue
            newExt = CMSSWConfigExtension(nodeName)
            newExt.load(extNode)
            self.extensions[nodeName] = newExt



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

        if self.extensions.has_key("SecondaryInputFiles"):
            cfg.inputSource.setSecondaryFileNames(
                *self.extensions["SecondaryInputFiles"].data)

        firstRun = self.sourceParams.get("firstRun", None)
        if firstRun != None:
            cfg.inputSource.setFirstRun(firstRun)

        skipEv = self.sourceParams.get("skipEvents", None)
        if skipEv != None:
            cfg.inputSource.setSkipEvents(skipEv)

        firstLumi = self.sourceParams.get("firstLuminosityBlock", None)
        if firstLumi != None:
            cfg.inputSource.setFirstLumi(firstLumi)

        firstEvent = self.sourceParams.get("firstEvent", None)
        if firstEvent != None:
            cfg.inputSource.setFirstEvent(firstEvent)

        cacheSize = self.sourceParams.get("cacheSize", None)
        if cacheSize != None:
            cfg.inputSource.setCacheSize(cacheSize)

        if self.inputOverrideCatalog not in (None, ''):
            cfg.inputSource.setOverrideCatalog(self.inputOverrideCatalog, 'override')

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

        #  //
        # // set conditions tag
        #//
        if self.conditionsTag != None:
            cfg.setConditionsTag(self.conditionsTag)




        #  //
        # //  output modules
        #//
        for outModName, outModData in self.outputModules.items():
            modRef = cfg.outputModules.get(outModName, None)
            if modRef == None:
                continue
            if outModData["fileName"] != None:
                modRef.setFileName(outModData["fileName"])
            if outModData["logicalFileName"] != None:
                modRef.setLogicalFileName(outModData["logicalFileName"])
            if outModData["catalog"] != None:
                modRef.setCatalog(outModData["catalog"])


        #  //
        # // Pileup Files
        #//
        cfg.insertPileupFiles(*self.pileupFiles)

        if len(self.beamHaloPlusFiles) > 0:
            cfg.setPileupFilesForSource("beamhalo_plus",
                                        *self.beamHaloPlusFiles)
        if len(self.beamHaloMinusFiles) > 0:
            cfg.setPileupFilesForSource("beamhalo_minus",
                                        *self.beamHaloMinusFiles)
        if len(self.cosmicPileupFiles) > 0:
            cfg.setPileupFilesForSource("cosmics",
                                        *self.cosmicPileupFiles)
        if len(self.dataMixerFiles) > 0:
            cfg.setPileupFilesForModule("input",
                                        'DataMixingModule',
                                        *self.dataMixerFiles)

        #  //
        # // TFileAdaptor
        #//
        if self.tFileAdaptorConfig:
            cfg.setTFileAdaptorConfig(self.tFileAdaptorConfig)

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

        #  //
        # // ConfigMetadata
        #//
        self.configMetadata.update(cfgInterface.configMetadata())

        #  //
        # // conditions tag
        #//
        self.conditionsTag = cfgInterface.getConditionsTag()

        #  //
        # // Pileup Files
        #//
        self.pileupFiles = cfgInterface.pileupFileList()

        #  //
        # // DataMix Pileup Files
        #//
        self.dataMixerFiles = cfgInterface.dataMixPileupFileList()


        return cfgInterface



