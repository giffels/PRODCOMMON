#!/usr/bin/env python
"""
_InputSource_

Object to assist with manipulating the input source provided in a PSet

"""

import FWCore.ParameterSet.Types as CfgTypes

from ProdCommon.CMSConfigTools.ConfigAPI.Utilities import _CfgNoneType


class InputSource:
    """
    _InputSource_

    Util for manipulating the InputSource within a CMS Config Object

    """
    def __init__(self, sourceRef):
        self.data = sourceRef
        self.sourceType = sourceRef.type_()

    def __str__(self):
        return self.data.dumpConfig()


    def maxevents(self):
        """get value of MaxEvents, None if not set"""
        cfgType = getattr(self.data, "maxEvents", _CfgNoneType())
        return cfgType.value()


    def setMaxEvents(self, maxEv):
        """setMaxEvents value"""
        self.data.maxEvents = CfgTypes.untracked(CfgTypes.int32(maxEv))

    def skipevents(self):
        """get value of SkipEvents, None if not set"""
        cfgType = getattr(self.data, "skipEvents", _CfgNoneType())
        return cfgType.value()

    def setSkipEvents(self, skipEv):
        "set SkipEvents value"""
        self.data.skipEvents = CfgTypes.untracked(CfgTypes.uint32(int(skipEv)))

    def firstRun(self):
        """get firstRun value of None if not set"""
        cfgType = getattr(self.data, "firstRun", _CfgNoneType())
        return cfgType.value()

    def setFirstRun(self, firstRun):
        """set first run number"""
        self.data.firstRun = CfgTypes.untracked(CfgTypes.uint32(int(firstRun)))

    def setNumberEventsInRun(self, numEvents):
        """
        set numberEventsInRun parameter
        """
        self.data.numberEventsInRun = CfgTypes.untracked(
            CfgTypes.uint32(numEvents))

    def setFirstLumi(self, lumiId):
        """
        set lumi id for this job
        """
        self.data.firstLuminosityBlock = CfgTypes.untracked(
            CfgTypes.uint32(int(lumiId)))

    def firstLumi(self):
        """
        get first lumi id value
        """
        cfgType = getattr(self.data, "firstLuminosityBlock", _CfgNoneType())
        return cfgType.value()

    def setFirstEvent(self, firstEv):
        """
        set first event number

        """
        self.data.firstEvent = CfgTypes.untracked(
            CfgTypes.uint32(int(firstEv)))

    def firstEvent(self):
        """
        get firstEvent value

        """
        cfgType = getattr(self.data, "firstEvent", _CfgNoneType())
        return cfgType.value()


    def fileNames(self):
        """ return value of fileNames, None if not provided """
        cfgType = getattr(self.data, "fileNames", _CfgNoneType())
        value = cfgType.value()
        if value == None:
            return []
        if type(value) == type("string"):
            return [value]
        return value

    def setFileNames(self, *fileNames):
        """set fileNames vector"""
        if fileNames:
            self.data.fileNames = CfgTypes.untracked(CfgTypes.vstring())
        for entry in fileNames:
            self.data.fileNames.append(entry)

        return

    def setSecondaryFileNames(self, *fileNames):
        """set secondary file names vector"""
        self.data.secondaryFileNames = CfgTypes.untracked(CfgTypes.vstring())
        for entry in fileNames:
            self.data.secondaryFileNames.append(entry)
        return


    def setFileMatchMode(self, matchMode):
        """set file match mode for reading files in same job"""
        self.data.fileMatchMode = CfgTypes.untracked(
            CfgTypes.string(matchMode))


    def overrideCatalog(self):
        """get overrideCatalog value of None if not set"""
        cfgType = getattr(self.data, "overrideCatalog", _CfgNoneType())
        return cfgType.value()


    def setOverrideCatalog(self, catalog, protocol):
        """set overrideCatalog to allow local files to be read via LFN"""
        uri = "trivialcatalog_file:%s?protocol=%s" % (catalog, protocol)
        self.data.overrideCatalog = CfgTypes.untracked(CfgTypes.string(uri))


    def lumisToProcess(self):
        """ return value of lumisToProcess, None if not provided """
        cfgType = getattr(self.data, "lumisToProcess", _CfgNoneType())
        value = cfgType.value()
        if value == None:
            return []
        if type(value) == type("string"):
            return [value]
        return value


    def setLumisToProcess(self, *lumiList):
        """set lumisToProcess vector"""
        self.data.lumisToProcess = CfgTypes.untracked(
            CfgTypes.VLuminosityBlockRange())
        for entry in lumiList:
            self.data.lumisToProcess.append(entry)

        return


    def cacheSize(self):
        """return value for source.cacheSize"""
        cfgType = getattr(self.data, "cacheSize", _CfgNoneType())
        return cfgType.value()


    def setCacheSize(self, size):
        """Set source cacheSize"""
        self.data.cacheSize = CfgTypes.untracked(CfgTypes.uint32(int(size)))


    def sourceParameters(self):
        """
        _sourceParamaters_

        Extract the source parameters that are pertinent to WM
        return them as a dictionary
        """
        result = {}
        result['fileNames'] = self.fileNames()
        result['lumisToProcess'] = self.lumisToProcess()
        result['firstRun'] = self.firstRun()
        result['skipEvents'] = self.skipevents()
        result['overrideCatalog'] = self.overrideCatalog()
        result['firstLuminosityBlock'] = self.firstLumi()
        result['firstEvent'] = self.firstEvent()
        result['cacheSize'] = self.cacheSize()
        return result
