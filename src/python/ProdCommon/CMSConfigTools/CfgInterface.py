#!/usr/bin/env python
"""
_CfgInterface_

Wrapper class for a cmsconfig object with interfaces to manipulate
the InputSource and output modules

"""

import copy

from ProdCommon.CMSConfigTools.cmsconfig import cmsconfig
from ProdCommon.CMSConfigTools.InputSource import InputSource
from ProdCommon.CMSConfigTools.OutputModule import OutputModule


class CfgInterface:
    """
    _CfgInterface_

    Wrapper object for a cmsconfig instance.
    Generates an InputSource object and OutputModules from the
    cfg file.

    Provides a clone interface that returns a new copy of itself to
    assist in generating jobs from a Template cfg file

    Ctor argument can be either the file containing the cfg file, or
    the content of it as a string. In the latter case, the second ctor
    arg must be provided as True.

    """
    def __init__(self, pyFormatCfgFile, isString = False):
        if not isString:
            cfgContent = file(pyFormatCfgFile).read()
        else:
            cfgContent = pyFormatCfgFile
            
        self.cmsConfig = cmsconfig(cfgContent)
        self.inputSource = InputSource(self.cmsConfig.mainInputSource())
        self.outputModules = {}
        for omodName in self.cmsConfig.outputModuleNames():
            self.outputModules[omodName] = OutputModule(
                omodName, self.cmsConfig.module(omodName)
                )

    def clone(self):
        """
        _clone_

        return a new instance of this object by copying it

        """
        return copy.deepcopy(self)

    
    def __str__(self):
        """string rep of self: give python format PSet"""
        return self.cmsConfig.asPythonString()

    def hackMaxEvents(self, value):
        """
        _hackMaxEvents_

        Drop the source maxEvents value into the cfg file PSet

        """
        
        
        maxEvents = self.cmsConfig.psdata['psets'].get(
            "maxEvents", None)
        if maxEvents == None:
            # no maxEvents PSet in cfg so skip
            return
        
        psetContent = self.cmsConfig.psdata['psets']['maxEvents'][2]
        psetContent['input'] = ('int32', 'untracked', int(value) )
        return
    
        
        
    def mixingModules(self):
        """
        _mixingModules_

        return refs to all mixing modules in the cfg

        """
        result = []
        for secSource in self.cmsConfig.moduleNamesWithSecSources():
            module = self.cmsConfig.module(secSource)
            if module['@classname'][2] == "MixingModule":
                result.append(module)
        return result
                
            
    def configMetadata(self):
        """
        _configMetadata_

        Get a dictionary of the configuration metadata from this cfg
        file if present

        """
        
        result = {}
        cMeta = self.cmsConfig.psdata['psets'].get(
            "configurationMetadata", ("", "", {}))[2]

        if cMeta.has_key("version"):
            version = cMeta['version'][2]
            version = version.replace("$", "")
            version = version.replace("\"", "")
            result['Version'] = version.strip()

        if cMeta.has_key("name"):
            name = cMeta["name"][2]
            name = name.replace("$", "")
            name = name.replace("\"", "")
            name = name.replace(",v", "")
            name = name.replace("Source", "")
            name = name.strip()
            result["Name"] = name


        if cMeta.has_key("annotation"):
            annot = cMeta["annotation"][2]
            annot = annot.replace("\"", "")
            result["Annotation"] = annot
            
        
            
        return result
            

