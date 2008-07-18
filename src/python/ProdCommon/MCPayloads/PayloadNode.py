#!/usr/bin/env python
"""
_PayloadNode_

Define a tree node class to represent a application step in a workflow.

Provides for a tree structure showing application ordering/dependencies
and allows for common attributes to be provided for each application
including:

Application Project, Version, Architecture
Input Datasets
Output Datasets
Configuration 

"""

import base64

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery

from ProdCommon.MCPayloads.DatasetInfo import DatasetInfo
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


def intersection(list1, list2):
    """fast intersection of two lists"""
    intDict = {}
    list1Dict = {}
    for entry in list1:
        list1Dict[entry] = 1
    for entry in list2:
        if list1Dict.has_key(entry):
            intDict[entry] = 1
    return intDict.keys()

def listAllNames(payloadNode):
    """
    _listAllNames_
    
    Generate a top-descent based list of all node names for
    all nodes in this node tree. Traverse to the topmost node first
    and then recursively list all the names in the tree.
    
    """
    if payloadNode.parent != None:
        return listAllNames(payloadNode.parent)
    return  payloadNode.listDescendantNames()


class NodeFinder:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance


def getNodeByName(payloadNode, nodeName):
    """
    _getNodeByName_

    Given a payload node, search the tree it is part of for the
    node with the name provided.

    """
    if payloadNode.parent != None:
        return getNodeByName(payloadNode.parent, nodeName)

    finder = NodeFinder(nodeName)
    payloadNode.operate(finder)
    return finder.result


class InputLink(dict):
    """
    _InputLink_

    Object that can be used to create an IO link dependency between
    two Payload Nodes. This object is added to the PayloadNode
    that will process the data.

    node1 -> node2
              - InputLink(node1)

    Contains:

    InputNode - name of the PayloadNode where the input is coming from
    OutputModule - name of the OutputModule in the input node that generates
                   the data to be input.

    InputSource - name of the PoolSource that the input data will be fed to.
    AppearStandalone - boolean whether to combine with the previous step and 
                        discard intermediate files
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("InputNode", None)
        self.setdefault("InputSource", None)
        self.setdefault("OutputModule", None)
        self.update(args)

    def save(self):
        result = IMProvNode("InputLink")
        for key, value in self.items():
            result.addNode(IMProvNode(key, None, Value = value))
        return result

    def load(self, improvNode):
        dataQ = IMProvQuery("/InputLink/*")
        dataNodes = dataQ(improvNode)
        for node in dataNodes:
            key = str(node.name)
            val = str(node.attrs['Value'])
            self[key] = val
            
        if self.get("AppearStandalone", "false").lower() in ("true", "yes"):
            self["AppearStandalone"] = True
        else:
            self["AppearStandalone"] = False
            
        return
    
                          

class PayloadNode:
    """
    _PayloadNode_

    Abstract Application entry in a tree like workflow model

    """
    def __init__(self, name = None):
        self.children = []
        self.parent = None
        self.name = None
        self.workflow = None
        if name != None:
            self.name = name
        self.type = None
        self.application = {}
        self.application.setdefault("Project", None)
        self.application.setdefault("Version", None)
        self.application.setdefault("Architecture", None)
        self.application.setdefault("Executable", None)
        self.applicationControls = {}
        self.applicationControls.setdefault("EventMultiplier", None)
        self.applicationControls.setdefault("SelectionEfficiency", None)
        self.applicationControls.setdefault("PerRunFraction", None)
        #  //
        # // These lists are deprecated and are maintained here
        #//  for backwards compatibility for short term
        self.inputDatasets = []
        self.outputDatasets = []
        

        self.scriptControls = {}
        self.scriptControls.setdefault("PreTask", [])
        self.scriptControls.setdefault("PreExe", [])
        self.scriptControls.setdefault("PostExe", [])
        self.scriptControls.setdefault("PostTask", [])
        
            

        #  //
        # // Dataset information is stored as DatasetInfo objects
        #//   
        self._InputDatasets = []
        self._OutputDatasets = []
        self._PileupDatasets = []
        self.configuration = ""
        self.cfgInterface = None
        self.userSandbox = None
        
        #  //
        # // Input Links to other nodes
        #//
        self._InputLinks = []
        

    def newNode(self, name):
        """
        _newNode_

        Create a new PayloadNode that is a child to this node
        and return it so that it can be configured.

        New Node name must be unique within the tree or it will barf

        """
        newNode = PayloadNode()
        newNode.name = name
        self.addNode(newNode)
        return newNode
    
    def addInputDataset(self, primaryDS, processedDS):
        """
        _addInputDataset_

        Add a new Input Dataset to this Node.
        Arguments should be:

        - *primaryDS* : The Primary Dataset name of the input dataset

        - *processedDS* : The Processed Dataset name of the input dataset

        The DatasetInfo object is returned by reference for more information
        to be added to it

        InputModuleName should be the mainInputSource of the PSet for
        the main input dataset. At present this is set elsewhere
        
        """
        newDataset = DatasetInfo()
        newDataset['PrimaryDataset'] = primaryDS
        newDataset['ProcessedDataset'] = processedDS
        self._InputDatasets.append(newDataset)
        return newDataset

    def addPileupDataset(self, primary, tier, processed):
        """
        _addPileupDataset_

        Add a pileup dataset to this node

        """
        newDataset = DatasetInfo()
        newDataset['PrimaryDataset'] = primary
        newDataset['DataTier'] = tier
        newDataset['ProcessedDataset'] = processed
        self._PileupDatasets.append(newDataset)
        return newDataset
        

    def addOutputDataset(self, primaryDS, processedDS, outputModuleName):
        """
        _addOutputDataset_

        Add a new Output Dataset, specifying the Primary and Processed
        Dataset names and the name of the output module in the PSet
        responsible for writing out files for that dataset

        
        """
        newDataset = DatasetInfo()
        newDataset['PrimaryDataset'] = primaryDS
        newDataset['ProcessedDataset'] = processedDS
        newDataset['OutputModuleName'] = outputModuleName
        self._OutputDatasets.append(newDataset)
        return newDataset
    
    def addInputLink(self, nodeName, nodeOutputModName,
                     thisNodeSourceName = None, AppearStandalone = False, skipCfgCheck = False):
        """
        _addInputLink_

        Add an input link between this node and another node above it in
        the tree. This means that output from the named output module 
        of the node will be linked to the source on this node. If a source name
        is not provided, the main source will be used
        

        """
        #  //
        # // Safety checks
        #//  1. Node name must exist
        if nodeName not in listAllNames(self):
            msg = "Error adding input link:  Node named %s " % nodeName
            msg += "Does not exist in the node tree"
            raise RuntimeError, msg
        #  //
        # // 2. Must be above this node. IE not in nodes descended from
        #//  this node
        if nodeName in self.listDescendantNames():
            msg = "Error adding input link: Node named %s \n" % nodeName
            msg += "Is below node %s in the tree\n" % self.name
            msg += "%s will run before %s\n" % (self.name, nodeName)
            raise RuntimeError, msg
        
        
            

        #  //
        # // TODO: Check if named source is present
        #//
        link = InputLink(InputNode = nodeName,
                         InputSource = thisNodeSourceName,
                         OutputModule = nodeOutputModName,
                         AppearStandalone = AppearStandalone)
        self._InputLinks.append(link)

        return
        

    def addNode(self, nodeInstance):
        """
        _addNode_

        Add a child node to this node
        nodeInstance must be an instance of PayloadNode

        """
        if not isinstance(nodeInstance, PayloadNode):
            msg = "Argument supplied to addNode is not a PayloadNode instance"
            raise RuntimeError, msg
        dupes = intersection(listAllNames(self), listAllNames(nodeInstance))
        if len(dupes) > 0:
            msg = "Duplicate Names already exist in parent tree:\n"
            msg += "The following names already exist in the parent tree:\n"
            for dupe in dupes:
                msg += "  %s\n" % dupe
            msg += "Each PayloadNode within the tree must "
            msg += "have a unique name\n"
            raise RuntimeError, msg
        self.children.append(nodeInstance)
        nodeInstance.workflow = self.workflow
        nodeInstance.parent = self
        return

    def listDescendantNames(self, result = None):
        """
        _listDescendantNames_

        return a list of all names of nodes below this node
        recursively traversing children
        """
        if result == None:
            result = []
        result.append(self.name)
        for child in self.children:
            result = child.listDescendantNames(result)
        return result
    
    def makeIMProv(self):
        """
        _makeIMProv_

        Serialise self and children into an XML DOM friendly node structure

        """
        node = IMProvNode(self.__class__.__name__, None, Name = str(self.name),
                          Type = str(self.type) ,
                          Workflow = str(self.workflow))
        appNode = IMProvNode("Application")
        for key, val in self.application.items():
            appNode.addNode(IMProvNode(key, None, Value = val))
        appConNode = IMProvNode("ApplicationControl")
        for key, val in self.applicationControls.items():
            if val == None:
                continue
            appConNode.addNode(IMProvNode(key, None, Value = val))

        inputNode = IMProvNode("InputDatasets")
        for inpDS in self._InputDatasets:
            inputNode.addNode(inpDS.save())
        outputNode = IMProvNode("OutputDatasets")
        for outDS in self._OutputDatasets:
            outputNode.addNode(outDS.save())
        pileupNode = IMProvNode("PileupDatasets")
        for puDS in self._PileupDatasets:
            pileupNode.addNode(puDS.save())

        inpLinksNode = IMProvNode("InputLinks")
        for iLink in self._InputLinks:
            inpLinksNode.addNode(iLink.save())

        scriptsNode = IMProvNode("ScriptControls")
        for key, scriptList in self.scriptControls.items():
            scriptListNode = IMProvNode("ScriptList", None, Name = key)
            [ scriptListNode.addNode(IMProvNode("Script", None, Value = x))
              for x in scriptList ]
            scriptsNode.addNode(scriptListNode)
            
        if self.cfgInterface == None:
            configNode = IMProvNode("Configuration",
                                    base64.encodestring(self.configuration),
                                    Encoding="base64")
        else:
            configNode = self.cfgInterface.save()
        
        node.addNode(appNode)
        node.addNode(appConNode)
        node.addNode(scriptsNode)
        node.addNode(inputNode)
        node.addNode(outputNode)
        node.addNode(pileupNode)
        node.addNode(inpLinksNode)
        node.addNode(configNode)
        
        if self.userSandbox != None:
            sandboxNode = IMProvNode("UserSandbox", self.userSandbox)
            node.addNode(sandboxNode)

        

        for child in self.children:
            node.addNode(child.makeIMProv())

        return node

    
        
        
    def __str__(self):
        """string rep for easy inspection"""
        return str(self.makeIMProv())
    

    def operate(self, operator):
        """
        _operate_

        Recursive callable operation over a payloadNode tree 
        starting from this node.

        operator must be a callable object or function, that accepts
        a single argument, that argument being the current node being
        operated on.

        """
        operator(self)
        for child in self.children:
            child.operate(operator)
        return
    
    def populate(self, improvNode):
        """
        _populate_

        Extract details of this node from improvNode and
        instantiate and populate any children found

        """
       
        self.unpackPayloadNodeData(improvNode)
        #  //
        # // Recursively handle children
        #//
        childQ = IMProvQuery("/PayloadNode/PayloadNode")
        childNodes = childQ(improvNode)
        for item in childNodes:
            newChild = PayloadNode()
            self.addNode(newChild)
            newChild.populate(item)

        
        return
        
    def unpackPayloadNodeData(self, improvNode):
        """
        _unpackPayloadNodeData_

        Unpack PayloadNode data from improv Node provided and
        add information to self

        """
        self.name = str(improvNode.attrs["Name"])
        self.type = str(improvNode.attrs["Type"])
        workflowName = improvNode.attrs.get('Workflow', None)
        if workflowName != None:
            self.workflow = str(workflowName)
        #  //
        # // Unpack data for this instance
        #//  App details
        appDataQ = IMProvQuery("/%s/Application" % self.__class__.__name__)
        appData = appDataQ(improvNode)[0]
        for appField in appData.children:
            field = str(appField.name)
            value = str(appField.attrs['Value'])
            self.application[field] = value
        #  //
        # // App Control details
        #//  
        appConDataQ = IMProvQuery("/%s/ApplicationControl/*" % self.__class__.__name__)
        appConData = appConDataQ(improvNode)
        for appConField in appConData:
            field = str(appConField.name)
            value = str(appConField.attrs['Value'])
            self.applicationControls[field] = value

        #  //
        # // Script Controls
        #//
        scriptConQ = IMProvQuery("/%s/ScriptControls/ScriptList" % self.__class__.__name__)
        scriptLists = scriptConQ(improvNode)
        for scriptList in scriptLists:
            listName = scriptList.attrs.get("Name", None)
            if listName == None: continue
            listName = str(listName)
            for script in scriptList.children:
                scriptName = script.attrs.get("Value", None)
                if scriptName == None: continue
                self.scriptControls[listName].append(str(scriptName))
                
        
        #  //
        # // Dataset details
        #//  Input Datasets
        inputDSQ = IMProvQuery(
            "/%s/InputDatasets/DatasetInfo" % self.__class__.__name__)
        inputDS = inputDSQ(improvNode)
#        print improvNode
        for item in inputDS:
            newDS = DatasetInfo()
            newDS.load(item)
            self._InputDatasets.append(newDS)

        #  //
        # // Output Datasets
        #//
        outputDSQ = IMProvQuery(
            "/%s/OutputDatasets/DatasetInfo" % self.__class__.__name__)
        outputDS = outputDSQ(improvNode)
        for item in outputDS:
            newDS = DatasetInfo()
            newDS.load(item)
            self._OutputDatasets.append(newDS)
        #  //
        # // Pileup Datasets
        #//
        pileupDSQ = IMProvQuery(
            "/%s/PileupDatasets/DatasetInfo" % self.__class__.__name__)
        pileupDS = pileupDSQ(improvNode)
        for item in pileupDS:
            newDS = DatasetInfo()
            newDS.load(item)
            self._PileupDatasets.append(newDS)
        #  //
        # // Input Links
        #//
        inpLinkQ = IMProvQuery(
            "/%s/InputLinks/InputLink" % self.__class__.__name__)
        inpLinks = inpLinkQ(improvNode)
        for ilink in inpLinks:
            newLink = InputLink()
            newLink.load(ilink)
            self._InputLinks.append(newLink)
            
        
        #  //
        # // Configuration
        #//
        configQ = IMProvQuery("/%s/Configuration" % self.__class__.__name__)
        configNodes = configQ(improvNode)
        if len(configNodes) > 0:
            configNode = configNodes[0]
            self.configuration = base64.decodestring(str(configNode.chardata))

        cfgIntQ = IMProvQuery("/%s/CMSSWConfig" % self.__class__.__name__)
        cfgNodes = cfgIntQ(improvNode)
        if len(cfgNodes) > 0:
            cfgNode = cfgNodes[0]
            self.cfgInterface = CMSSWConfig()
            self.cfgInterface.load(cfgNode)
            
        #  //
        # // User sandbox
        #//
        sandboxQ = IMProvQuery("/%s/UserSandbox" % self.__class__.__name__)
        sandboxNodes = sandboxQ(improvNode)
        if len(sandboxNodes) > 0:
            sandboxNode = sandboxNodes[-1]
            self.userSandbox = str(sandboxNode.chardata)
        
        return
    
