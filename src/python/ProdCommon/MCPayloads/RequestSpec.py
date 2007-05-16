#!/usr/bin/env python
"""
_RequestSpec_

Object that contains a request specification.

A RequestSpec contains:

1. The details of the request (if different from whatever defaults) such as
total size, thresholds and policies to be used

2. The WorkflowSpec instance as a complete nested entity

This can also be serialised into XML

"""
import logging

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

class RequestSpec:
    """
    _RequestSpec_

    Object + API for passing around details of a request, including the
    workflow itself and the extra details of the request

    """
    def __init__(self):
        self.workflow = None
        self.requestDetails = {}
        self.policies = {}
        self.preferredPAs = {}



    def save(self):
        """
        _save_

        Convert this object into an IMProvNode structure

        """
        result = IMProvDoc("RequestSpec")


        details = IMProvNode("RequestDetails")
        for key, val in self.requestDetails.items():
            details.addNode(IMProvNode(key, str(val)))

        policies = IMProvNode("Policies")
        for key, val in self.policies.items():
            policies.addNode(IMProvNode(key, str(val)))
            
            
        result.addNode(details)
        result.addNode(policies)
        result.addNode(self.workflow.makeIMProv())
        return result
        


    def load(self, improvNode):
        """
        _load_

        Extract information for this object from the improv instance provided
        """
        wfQuery = IMProvQuery("/RequestSpec/WorkflowSpec")
        wfnode = wfQuery(improvNode)[0]
        wfspec = WorkflowSpec()
        wfspec.loadFromNode(wfnode)
        self.workflow = wfspec

        policyQuery = IMProvQuery("/RequestSpec/Policies/*")
        detailQuery = IMProvQuery("/RequestSpec/RequestDetails/*")
        preferredPAQuery = IMProvQuery("/RequestSpec/PreferredPA")

        policies = policyQuery(improvNode)
        details = detailQuery(improvNode)
        preferredPAs = preferredPAQuery(improvNode)

        for policy in policies:
            self.policies[str(policy.name)] = str(policy.chardata)

        for detail in improvNode.attrs.keys():
            self.requestDetails[detail] = str(improvNode.attrs[detail])
 
        for preferredPA in preferredPAs:
            self.preferredPAs[str(preferredPA.attrs['id'])] = \
                str(preferredPA.attrs['priority'])

        return
        
        

    def write(self, filename):
        """
        _write_

        Write this object to XML file provided

        """
        handle = open(filename, 'w')
        handle.write(self.save().makeDOMDocument().toprettyxml())
        handle.close()
        return

    def read(self, filename):
        """
        _read_

        Given the filename, read the contents and populate self

        """
        node = loadIMProvFile(filename)
        self.load(node)
        return


    def __str__(self):
        """string rep is XML"""
        return self.save().makeDOMDocument().toprettyxml()



    
        
def readSpecFile(filename):
    """
    _readSpecFile_

    Tool for extracting multiple RequestSpecs from a file

    Returns a list of spec instances found
    """
    result = []
    # ignore failed parsed requests
    ignore = 0
  
    node = loadIMProvFile(filename)
    node = loadIMProvFile(filename)
    specQ = IMProvQuery("RequestSpec")
    specNodes = specQ(node)

    for snode in specNodes:
        newSpec = RequestSpec()
        try: 
           newSpec.load(snode)
           result.append(newSpec)
        except Exception,ex:
           logging.debug('ERROR loading a requestspec, will ignore this requestspec :'+str(ex))
           ignore+=1
    return (result,ignore)

def writeSpecFile(filename, *specInstances):
    """
    _writeSpecFile_

    Util to write multiple specs to a single file

    """
    doc = IMProvDoc("RequestSpecs")
    for spec in specInstances:
        doc.addNode(spec.save())

    handle = open(filename, 'w')
    handle.write(doc.makeDOMDocument().toprettyxml())
    handle.close()
    return




    
