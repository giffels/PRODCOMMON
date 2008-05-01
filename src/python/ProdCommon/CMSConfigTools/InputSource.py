#!/usr/bin/env python
"""
_InputSource_

Object to assist with manipulating the input source provided in a PSet

"""

from ProdCommon.CMSConfigTools.Utilities import isQuoted

class InputSource:
    """
    _InputSource_

    Util for manipulating the InputSource within a cmsconfig object

    """
    def __init__(self, sourceDictRef):
        self.data = sourceDictRef
        self.sourceType = sourceDictRef['@classname'][2]

    def maxevents(self):
        """get value of MaxEvents, None if not set"""
        tpl = self.data.get("maxEvents", None)
        if tpl != None:
            return int(tpl[2])
        return None
    
    def setMaxEvents(self, maxEv):
        """setMaxEvents value"""
        self.data['maxEvents'] = ('int32', 'untracked', maxEv)

    def skipevents(self):
        """get value of SkipEvents, None if not set"""
        tpl = self.data.get("skipEvents", None)
        if tpl != None:
            return int(tpl[2])
        return None

    def setSkipEvents(self, skipEv):
        "set SkipEvents value"""
        self.data['skipEvents'] = ('uint32', 'untracked', skipEv)

    def firstRun(self):
        """get firstRun value of None if not set"""
        tpl = self.data.get("firstRun", None)
        if tpl != None:
            return int(tpl[2])
        return None
        
    def setFirstRun(self, firstRun):
        """set first run number"""
        self.data['firstRun'] = ('uint32', 'untracked', firstRun)

    def setNumberEventsInRun(self, numEvents):
        """
        set numberEventsInRun parameter
        """
        self.data['numberEventsInRun'] = ('uint32', 'untracked', numEvents)
        
    def fileNames(self):
        """ return value of fileNames, None if not provided """
        tpl = self.data.get("fileNames", None)
        if tpl != None:
            return tpl[2]
        return None

    def setFileNames(self, *fileNames):
        """set fileNames vector"""
        fnames = []
        for fname in fileNames:
            if not isQuoted(fname):
                fname = "\'%s\'" % fname
            fnames.append(fname)
        self.data['fileNames'] = ('vstring', 'untracked', fnames)
        
    def setFileMatchMode(self, matchMode):
        """set file match mode for reading files in same job"""
        if not isQuoted(matchMode):
          matchMode =  "\'%s\'" % matchMode
        self.data['fileMatchMode'] = ('string', 'untracked', matchMode)

    def overrideCatalog(self):
        """get value of overrideCatalog, None if not set"""
        tpl = self.data.get("overrideCatalog", None)
        if tpl != None:
            return tpl[2]
        return None
    
    def setOverrideCatalog(self, catalog, protocol):
        """setMaxEvents value"""
        uri = "trivialcatalog_file:%s?protocol=%s" % (catalog, protocol)
        self.data['overrideCatalog'] = ('string', 'untracked', uri)

