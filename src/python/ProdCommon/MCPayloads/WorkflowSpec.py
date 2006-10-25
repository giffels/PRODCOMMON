#!/usr/bin/env python
"""
_WorkflowSpec_

Specification object to define a workflow, or top level request.

This specification consists of a tree of PayloadNodes that specify
the general form of the job with broad details such as
applications to be run, app ordering, general app configuration and
dataset ids for input and output between steps.

"""

import time

from MCPayloads.PayloadNode import PayloadNode
from MCPayloads.JobSpecNode import JobSpecNode
from MCPayloads.JobSpec import JobSpec
from MCPayloads.DatasetExpander import expandDatasetInfo
import  MCPayloads.DatasetTools as DatasetTools

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

class WorkflowSpec:
    """
    _WorkflowSpec_

    Broad definition of a workflow tree using PayloadNodes to
    represent the workflow as a tree.

    """
    def __init__(self):
        self.payload = PayloadNode()
        self.parameters = {}
        self.parameters.setdefault("WorkflowName", "Workflow-%s" % time.time())
        self.parameters.setdefault("RequestTimestamp", int(time.time()))
        self.parameters.setdefault("RequestCategory", "PreProd") 
        self._NodeMap = {}

    def workflowName(self):
        """
        _workflowName_

        Get the Workflow Name for this instance
        
        """
        return self.parameters["WorkflowName"]

    def setWorkflowName(self, name):
        """
        _setWorkflowName_

        Set the name of this workflow
        """
        self.parameters['WorkflowName'] = name
        updateWorkflowName(self.payload, name)
        return

    def requestTimestamp(self):
        """
        _requestTimestamp_

        Get the request timestamp that will be used in LFN generation

        """
        return int(self.parameters['RequestTimestamp'])

    def setRequestTimestamp(self, timestamp):
        """
        _setRequestTimestamp_

        Set the Request Timestamp to be used for LFN creation. Must be
        a Unix time integer such as the output of time.time() in python
        or `date +%s`
        
        """
        self.parameters['RequestTimestamp'] = timestamp
        return

    def requestCategory(self):
        """
        _requestCategory_

        Get the Request Category value. This is the broad classification
        of the Request indicating what it is for, eg PreProd, CSA06, DC04,
        Personal etc etc

        """
        return self.parameters['RequestCategory']

    def setRequestCategory(self, category):
        """
        _setRequestCategory_

        Set the category value for this request

        """
        self.parameters['RequestCategory'] = category
        return
    
    def makeIMProv(self):
        """
        _makeIMProv_

        Serialise the WorkflowSpec instance into an XML IMProv structure

        """
        node = IMProvNode("WorkflowSpec")
        for key, val in self.parameters.items():
            paramNode = IMProvNode("Parameter", str(val), Name = str(key))
            node.addNode(paramNode)
        payload = IMProvNode("Payload")
        payload.addNode(self.payload.makeIMProv())
        node.addNode(payload)
        return node
    

    def save(self, filename):
        """
        _save_

        Save this workflow spec into a file using the name provided

        """
        handle = open(filename, 'w')
        handle.write(self.makeIMProv().makeDOMElement().toprettyxml())
        handle.close()
        return

    def load(self, filename):
        """
        _load_

        Load a saved WorkflowSpec from a File

        """
        node = loadIMProvFile(filename)
        paramQ = IMProvQuery("/WorkflowSpec/Parameter")
        payloadQ = IMProvQuery("/WorkflowSpec/Payload/PayloadNode")

        #  //
        # // Extract Params
        #//
        paramNodes = paramQ(node)
        for item in paramNodes:
            paramName = item.attrs.get("Name", None)
            if paramName == None:
                continue
            paramValue = str(item.chardata)
            self.parameters[str(paramName)] = paramValue


        #  //
        # // Extract Payload Nodes
        #//
        payload = payloadQ(node)[0]
        self.payload = PayloadNode()
        self.payload.populate(payload)
        return
        
    def createJobSpec(self):
        """
        _createJobSpec_

        Create a tree of JobSpecNodes from this WorkflowSpec
        instances tree of payload nodes.
        Same tree structure will be used, but nodes will be
        JobSpecNode instances containing the details from the
        corresponding PayloadNode in this instance.

        To be used to create JobSpecNode trees from a Workflow
        to represent a job created from the general workflow

        """
        self._NodeMap = {}
        result = JobSpec()
        result.payload = self._CloneTreeNode(self.payload)
        return result
    

    
    def _CloneTreeNode(self, node):
        """
        _CloneTreeNode_

        Internal method for cloning a PayloadNode as a JobSpecNode 
        and keeping track of the parentage via an id based hashtable
        
        """
        newNode = JobSpecNode()
        newNode.loadPayloadNode(node)
        for key, val in self.parameters.items():
            newNode.addParameter(key, val)
        self._NodeMap[id(node)] = newNode
        if node.parent != None:
            parentNode = self._NodeMap[id(node.parent)]
            parentNode.addNode(newNode)
            
        for child in node.children:
            self._CloneTreeNode(child)
        return newNode
    
    
    #  //
    # // Accessor methods for retrieving dataset information
    #//
    def outputDatasets(self):
        """
        _outputDatasets_

        returns a list of MCPayload.DatasetInfo objects (essentially
        just dictionaries) containing all of the output datasets
        in all nodes of this WorkflowSpec

        """
        allDatasets = DatasetTools.getOutputDatasetsFromTree(self.payload)
        result = []
        #  //
        # // Split multi tiers into basic tiers
        #//
        for dataset in allDatasets:
            result.extend(expandDatasetInfo(dataset, self.requestTimestamp()))
        return result
    
    

def updateWorkflowName(payloadNode, workflowName):
    """
    _updateWorkflowName_

    Transmit change in workflow name to all existing payload nodes

    """
    payloadNode.workflow = workflowName
    for child in payloadNode.children:
        updateWorkflowName(child, workflowName)
    return

