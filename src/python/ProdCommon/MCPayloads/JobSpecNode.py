#!/usr/bin/env python
"""
_JobSpecNode_

Specialisation of a PayloadNode to include details
of a concretised job node for a set number of events.

This is accomplished through the aggregation of a JobSpecNode with the
PayloadNode to include details of the concrete
job such as seeds, run number, event totals, lfns etc.

"""

import copy

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery

from ProdCommon.MCPayloads.PayloadNode import PayloadNode

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

        
class JobSpecNode(PayloadNode):
    """
    _JobSpecNode_

    Specialised PayloadNode that contains concrete details
    of an actual job using the generic content of the PayloadNode

    
    """
    def __init__(self):
        PayloadNode.__init__(self)
        self.jobName = None
        self.jobType = None
        self.parameters = []
        self.inputModule = None
        self.outputModules = []

        

    def addNode(self, nodeInstance):
        """
        _addNode_

        Add JobSpecNode to this object as a child, propaget jobName

        """
        PayloadNode.addNode(self, nodeInstance)
        nodeInstance.jobName = self.jobName
        nodeInstance.jobType = self.jobType
        return

    def loadPayloadNode(self, genericPayload):
        """
        _loadPayloadNode_

        Populate self with details of the general PayloadNode
        provided
        
        """
        self.name = genericPayload.name
        self.type = genericPayload.type
        self.workflow = genericPayload.workflow
        self.application = genericPayload.application
        self.applicationControls = genericPayload.applicationControls
        self.scriptControls = genericPayload.scriptControls
        self._InputDatasets = genericPayload._InputDatasets
        self._OutputDatasets = genericPayload._OutputDatasets
        self.configuration = genericPayload.configuration
        self.cfgInterface = copy.copy(genericPayload.cfgInterface)
        self._InputLinks = genericPayload._InputLinks
        self.userSandbox = genericPayload.userSandbox
        self.loadConfiguration()


    def loadConfiguration(self):
        """
        _loadConfiguration_

        Test the configuration to see if it is a PSet, if so
        load it up and populate the data members related to it

        """
        if self.cfgInterface == None:
            return
        #  //
        # // Pull info out of the config if available
        #//
        self.extractOutputModules()
        self.extractInputSource()
        return

    

    def addParameter(self, paramName, paramValue):
        """
        _addParameter_

        add a key value parameter pair. These are stored as a list
        so duplicates are possible, for instance to allow multiple
        Seeds etc

        """
        self.parameters.append( (paramName, paramValue))
        return

    def hasParameter(self, paramName):
        """
        _hasParameter_

        Return true if there are parameters with the name provided in
        this job spec node

        """
        for param in self.parameters:
            if param[0] == paramName:
                return True
        return False

    def getParameter(self, paramName):
        """
        _getParameter_

        Returns value(s) for parameters with paramName as a list

        """
        result = []
        for item in self.parameters:
            if item[0] == paramName:
                result.append(item[1])
        return result

    def extractOutputModules(self):
        """
        _extractOutputModules_

        if the configuration is a python PSet, extract the list of
        output modules from it and add them to this instance
        

        """
        if self.cfgInterface == None:
            return []
        self.outputModules = []
        self.outputModules.extend(self.cfgInterface.outputModules.keys())
        return self.outputModules
    
    
        
    

    def extractInputSource(self):
        """
        _extractInputSource_

        If the configuration attribute is a python PSet, extract
        the main input source from it and add it to this instance
        
        """
        if self.cfgInterface == None:
            return None
        
        self.inputModule = self.cfgInterface.sourceType
        return self.inputModule
    
        
    def makeIMProv(self):
        """
        _makeIMProv_

        Generate IMProvNode based persistency object

        """
        baseNode = PayloadNode.makeIMProv(self)
        baseNode.attrs['JobName'] = self.jobName
        baseNode.attrs['JobType'] = self.jobType
        specNode = IMProvNode("JobSpecification")

        paramsNode = IMProvNode("Parameters")
        for item in self.parameters:
            paramsNode.addNode(IMProvNode(item[0], str(item[1])))
            
        outNode = IMProvNode("OutputModules")
        for outMod in self.outputModules:
            outNode.addNode(IMProvNode("OutputModule", str(outMod)))
            
        specNode.addNode(paramsNode)
        specNode.addNode(IMProvNode("InputSource", str(self.inputModule)))
        specNode.addNode(outNode)
        
        baseNode.addNode(specNode)
        return baseNode
            
    def populate(self, improvNode):
        """
        _populate_

        Override PayloadNode.populate to load a saved JobSpecNode

        """
        #  //
        # // Unpack base class data
        #//
        self.unpackPayloadNodeData(improvNode)
        jobName = improvNode.attrs.get("JobName", None)
        if jobName != None:
            self.jobName = str(jobName)
        jobType = improvNode.attrs.get("JobType", None)
        if jobType != None:
            self.jobType = str(jobType)
            
        
        #  //
        # // Unpack JobSpecNode additional Data
        #//  Parameters
        paramQ = IMProvQuery("/JobSpecNode/JobSpecification/Parameters/*")
        params = paramQ(improvNode)
        for paramNode in params:
            key = str(paramNode.name)
            value = str(paramNode.chardata)
            self.addParameter(key, value)

        #  //
        # // InputModule 
        #//
        inpQ = IMProvQuery("/JobSpecNode/JobSpecification/InputModule")
        inputModNode = inpQ(improvNode)
        if len(inputModNode) > 0:
            inputModNode = inputModNode[0]
            self.inputModule = str(inputModNode.chardata)
            

        #  //
        # // OutputModules
        #//
        outQ = IMProvQuery(
            "/JobSpecNode/JobSpecification/OutputModules/OutputModule")
        outMods = outQ(improvNode)
        for outMod in outMods:
            self.outputModules.append(str(outMod.chardata))
            
        
        #  //
        # // Recursively handle children
        #//
        childQ = IMProvQuery("/JobSpecNode/JobSpecNode")
        childNodes = childQ(improvNode)
        for item in childNodes:
            newChild = JobSpecNode()
            self.addNode(newChild)
            newChild.populate(item)
            
            
        return

        

#  //
# // Below util classes arent needed since all the information is
#//  contained in the python PSet and is easily extractable
#  //Will keep class defs in here as comments though, in case they
# // need resurrected for some reason I havent thought of
#//

##class InputModule(dict):
##    """
##    _InputModule_

##    Dictionary based class to represent an Input Module in the
##    framework including lists of files, input catalogs,
##    and event read maximum

##    """
##    def __init__(self):
##        dict.__init__(self)
##        self.setdefault("ModuleType", None)
##        self.setdefault("InputFiles", [])
##        self.setdefault("Catalog", None)
##        self.setdefault("MaxEvents", -1)
        
##    def makeIMProv(self):
##        """
##        _makeIMProv_

##        Return a persistent form of this object and its state
##        as an IMProvNode object

##        """
##        result = IMProvNode(self.__class__.__name__)
##        result.addNode(
##            IMProvNode("MaxEvents", None, Value = str(self['MaxEvents']))
##            )
##        result.addNode(IMProvNode("ModuleType", str(self['ModuleType'])))
##        result.addNode(IMProvNode("Catalog", str(self['Catalog'])))
##        fileNode = IMProvNode("InputFiles")
##        for item in self['InputFiles']:
##            fileNode.addNode(IMProvNode("File", str(item)))
##        result.addNode(fileNode)
##        return result

##    def populate(self, node):
##        """
##        _populate_

##        Fill self with data contained in node provided
##        """
##        maxEvQ = IMProvQuery(
##            "/%s/MaxEvents[attribute(\"Value\")]" % self.__class__.__name__)
##        maxEv = maxEvQ(node)[0]
##        self['MaxEvents'] = int(maxEv)

##        catalog = IMProvQuery(
##            "/%s/Catalog[text()]" % self.__class__.__name__)(node)[0]
##        value = str(catalog)
##        self['Catalog'] = None
##        if value != 'None':
##            self['Catalog'] = value

##        modType = IMProvQuery(
##            "/%s/ModuleType[text()]" % self.__class__.__name__)(node)[0]
##        value = str(modType)
##        self['ModuleType'] = None
##        if value != 'None':
##            self['ModuleType'] = value
            
##        files = IMProvQuery(
##            "/%s/InputFiles/File[text()]" % self.__class__.__name__)(node)
##        for item in files:
##            self['InputFiles'].append(str(item))

##        return
            
            
        
        
        
        
##class OutputModule(dict):
##    """
##    _OutputModule_

##    Dict based class to represent an output module including
##    lists of output files, catalogs etc.

##    """
##    def __init__(self):
##        dict.__init__(self)
##        self.setdefault("ModuleType", None)
##        self.setdefault("ModuleName", None)
##        self.setdefault("OutputCatalog", "PoolFileCatalog.xml")
##        self.setdefault("PhysicalFileNames", [])
##        self.setdefault("LogicalFileNames", [])

##    def makeIMProv(self):
##        """
##        _makeIMProv_

##        Return a persistent form of this object and its state
##        as an IMProvNode object
        
##        """
##        result = IMProvNode(self.__class__.__name__)

##        result.addNode(
##            IMProvNode("ModuleType", str(self['ModuleType']))
##            )
##        result.addNode(
##            IMProvNode("ModuleName", str(self['ModuleName']))
##            )

##        result.addNode(
##            IMProvNode("OutputCatalog", self['OutputCatalog'])
##            )
##        pfns = IMProvNode("PhysicalNames")
##        for item in self['PhysicalFileNames']:
##            pfns.addNode(IMProvNode("PFN", str(item)))
##        lfns = IMProvNode("LogicalFiles")
##        for item in self['LogicalFileNames']:
##            lfns.addNode(IMProvNode("LFN", str(item)))
            
##        result.addNode(pfns)
##        result.addNode(lfns)
##        return result

##    def populate(self, improvNode):
##        """
##        _populate_

##        Fill data in from improvNode for this object

##        """
##        mType = IMProvQuery("/OutputModule/ModuleType[text()]")(improvNode)[0]
##        self['ModuleType'] = str(mType)

##        mName = IMProvQuery("/OutputModule/ModuleName[text()]")(improvNode)[0]
##        self['ModuleName'] = str(mName)

##        cat = IMProvQuery("/OutputModule/OutputCatalog[text()]")(improvNode)[0]
##        self['OutputCatalog'] = str(cat)

##        pfns = IMProvQuery(
##            "/OutputModule/PhysicalNames/PFN[text()]")(improvNode)
##        for pfn in pfns:
##            self['PhysicalFileNames'].append(str(pfn))
##        lfns = IMProvQuery(
##            "/OutputModule/LogicalNames/LFN[text()]")(improvNode)
##        for lfn in lfns:
##            self['LogicalFileNames'].append(str(lfn))
            
##        return
            

