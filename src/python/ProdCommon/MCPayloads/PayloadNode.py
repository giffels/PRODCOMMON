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

from MCPayloads.DatasetInfo import DatasetInfo

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

        #  //
        # // These lists are deprecated and are maintained here
        #//  for backwards compatibility for short term
        self.inputDatasets = []
        self.outputDatasets = []

        #  //
        # // Dataset information is stored as DatasetInfo objects
        #//   
        self._InputDatasets = []
        self._OutputDatasets = []
        
        self.configuration = ""

        

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
        inputNode = IMProvNode("InputDatasets")
        for inpDS in self._InputDatasets:
            inputNode.addNode(inpDS.save())
        outputNode = IMProvNode("OutputDatasets")
        for outDS in self._OutputDatasets:
            outputNode.addNode(outDS.save())
        
        configNode = IMProvNode("Configuration",
                                base64.encodestring(self.configuration),
                                Encoding="base64")
        node.addNode(appNode)
        node.addNode(inputNode)
        node.addNode(outputNode)
        node.addNode(configNode)

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
        # // Configuration
        #//
        configQ = IMProvQuery("/%s/Configuration" % self.__class__.__name__)
        configNode = configQ(improvNode)[0]
        self.configuration = base64.decodestring(str(configNode.chardata))
        
        
        return
    
