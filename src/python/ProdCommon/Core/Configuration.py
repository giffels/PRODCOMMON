#!/usr/bin/env python
"""
_Configuration_


"""
__revision__ = "$Id: Configuration.py,v 1.1 2006/09/01 02:27:29 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"


import os

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile


def loadProdCommonConfiguration():
    """
    _loadProdCommonConfiguration_

    Util method to load the ProdCommonConfiguration from a location
    defined by the env var PRODCOMMON_CONFIG

    Returns a ProdCommonConfiguration object
    
    """
    envVar = os.environ.get("PRODCOMMON_CONFIG", None)
    if envVar == None:
        msg = "Cannot load ProdCommon Configuration:\n"
        msg += "PRODCOMMON_CONFIG is not set:\n"
        raise RuntimeError, msg
    if not os.path.exists(envVar):
        msg = "File Not Found:\n"
        msg += "%s\n" % envVar
        msg += "PRODCOMMON_CONFIG must point to a valid file\n"
        raise RuntimeError, msg

    config = ProdCommonConfiguration()
    config.loadFromFile(envVar)
    return config

    

class ProdCommonConfiguration(dict):
    """
    _ProdCommonConfiguration_

    Configuation container for the ProdCommon

    Essentially a map of named ConfigBlock instances.

    There are two types of ConfigBlock:
    - Those for Core pieces of the ProdCommon (like ProdCommonDB etc)
    - Those for ProdCommon Components

    The Core ConfigBlocks are added by default, Component ConfigBlocks
    are added with the newComponentConfig method.
    No Components are added by default.

    The config file is used to provide a list of Components to be
    started to the prodAgentd startup utility. So only components
    to actually be run should be added to the config.

    """
    def __init__(self):
        dict.__init__(self)
        self.components = []
        #  //
        # // Core non-component pieces are included by default
        #//
        self.setdefault("DB", ConfigBlock("DB"))
        self.setdefault("Core", ConfigBlock("Core"))
        
    def save(self):
        """
        _save_

        Generate an IMProvNode object to save this object to XML

        """
        result = IMProvNode("ProdCommonConfiguration")
        for item in self.components:
            result.addNode(IMProvNode("Component", None,
                                      Name = item))
        for item in self.values():
            result.addNode(item.save())
            
        return result

    def load(self, improvNode):
        """
        _load_

        Populate self based on content of improvNode instance
        """
        componentQ = IMProvQuery(
            "ProdCommonConfiguration/Component[attribute(\"Name\")]"
            )
        configQ = IMProvQuery(
            "ProdCommonConfiguration/ConfigBlock"
            )
        components = componentQ(improvNode)
        configs = configQ(improvNode)
        for comp in components:
            self.components.append(comp)
        for config in configs:
            cfgBlock = ConfigBlock("temp")
            cfgBlock.load(config)
            self[cfgBlock.name] = cfgBlock
        
        return

    def loadFromFile(self, filename):
        """
        _loadFromFile_

        Read the file provided to extract the configuration

        """
        try:
            improv = loadIMProvFile(filename)
        except StandardError, ex:
            msg = "Cannot read file: %s\n" % filename
            msg += "Failed to load ProdCommonConfiguration\n"
            raise RuntimeError, msg

        self.load(improv)
        return

    def saveToFile(self, filename):
        """
        _saveToFile_

        Save this instance to the file provided

        """
        doc = IMProvDoc("ProdCommonConfig")
        doc.addNode(self.save())
        handle = open(filename, 'w')
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return
        

    def configNames(self):
        """
        _configNames_

        get list of all ConfigBlock names in this instance

        """
        return self.keys()
        
    def newComponentConfig(self, configName):
        """
        _newComponentConfig_
        
        Get a new ConfigBlock instance for a component of the prodAgent.
        configName must not exist yet.
        
        """
        if configName in self.configNames():
            msg = "Duplicate Config Name added to ProdCommonConfiguration\n"
            msg += "%s already exists\n" % configName
            raise RuntimeError, msg

        newCfg = ConfigBlock(configName)
        self[configName] = newCfg
        self.components.append(configName)
        return newCfg
    

    
    def getConfig(self, name):
        """
        _getConfig_

        Get the ConfigBlock for the name provided
        Returns None if not found
        """
        return self.get(name, None)
    
    def listComponents(self):
        """
        _listComponents_

        Get list of all components in this Config
        Does not include core non component config blocks such as
        ProdCommonDB blocks
        
        """
        return self.components
        
    

    def __str__(self):
        """string rep of config"""
        return str(self.save())


    
    
        
class ConfigBlock(dict):
    """
    _ConfigBlock_

    Dict based named configuration block to contain a group
    of parameters for a configuration object

    A single instance should correspond to a single Component.
    If key/value pairs is insufficient to configure a component
    (Eg it needs a list etc) then please let me know and we can add
    the appropriate functionality. For now, just keeping it simple...
    
    """
    def __init__(self, name):
        self.name = name
        self.comment = None
        dict.__init__(self)

    def save(self):
        """
        _save_

        Generate an IMProvNode containing the configuration for this
        Component
        
        """
        result = IMProvNode("ConfigBlock", None,
                            Name = self.name)
        if self.comment != None:
            result.addNode(IMProvNode("Comment", self.comment))
        for key, val in self.items():
            result.addNode(IMProvNode("Parameter", None,
                                      Name = str(key), Value = str(val))
                           )
        return result
    
    def load(self, improvNode):
        """
        _load_

        Populate self based on content of improvNode instance
        """
        nameQ = IMProvQuery("/ConfigBlock[attribute(\"Name\")]")
        paramQ = IMProvQuery("/ConfigBlock/Parameter")
        commentQ = IMProvQuery("/ConfigBlock/Comment[text()]")
        
        self.name = str(nameQ(improvNode)[0])
        comments = commentQ(improvNode)
        if len(comments) > 0:
            self.comment = str(comments[0])
        else:
            self.comment = None
        
        params = paramQ(improvNode)
        for paramNode in params:
            paramName = str(paramNode.attrs['Name'])
            paramVal = str(paramNode.attrs['Value'])
            self[paramName] = paramVal
            
        return

        
