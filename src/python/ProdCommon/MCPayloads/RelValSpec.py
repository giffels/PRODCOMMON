#!/usr/bin/env python
"""
_RelValSpec_

Object for interacting with a Release Validation Spec XML file.

Can be used to generate and save a spec, and also load it and iterate over it

"""

from xml.sax import make_parser

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import IMProvHandler

class RelValTest(dict):
    """
    _RelValTest_

    Dictionary containing fields describing a test

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault('Events' , None)
        self.setdefault('Name', None)
        self.setdefault("CfgUrl", None)
        self.setdefault("FractionSelected", None)
        self.setdefault("InputDataset", None)

    def save(self):
        """self to improv"""
        result = IMProvNode("Test", self["CfgUrl"],
                            Events = str(self['Events']),
                            Name = str(self['Name']),
                            )
        if self['FactionSelected'] != None:
            result.attrs['FractionSelected'] = str(self['FractionSelected'])

        if self['InputDataset'] != None:
            result.attrs['InputDataset'] = self['InputDataset']
        return result

    def load(self, improvNode):
        """improv to self"""
        self['Events'] = str(improvNode.attrs["Events"])
        self['Name'] = str(improvNode.attrs['Name'])
        self['CfgUrl'] = str(improvNode.chardata)
        if improvNode.attrs.has_key("FractionSelected"):
            self['FractionSelected'] = str(improvNode.attrs['FractionSelected'])
        if improvNode.attrs.has_key("InputDataset"):
            self['InputDataset'] = str(improvNode.attrs['InputDataset'])
        return



class RelValidation(list):
    """
    _RelValidation_

    A collection of tests for a single release.
    
    """
    def __init__(self, release):
        list.__init__(self)
        self.release = release


    def addTest(self, name, numEvents, cfgUrl, inpDataset = None):
        """
        _addTest_

        Add a new test for this release:

        - *name* : Name of the test, this will be used as the request and
        dataset name. Eg RelValSingleMuMinusPt100, RelValHiggs-ZZ-4Mu etc

        - *numEvents* : Total number of events for this test. Eg 500, 1000

        - *cfgUrl* : http based url for the cfg file for this test

        - *inpDataset* : Optional Input dataset name

        """
        newTest = RelValTest()
        newTest['Name'] = name
        newTest['Events'] = numEvents
        newTest['CfgUrl'] = cfgUrl
        if inpDataset != None:
            newTest['InputDataset'] = inpDataset
        #  //
        # // NOTE: Should check for dupe names here?
        #//
        self.append(newTest)
        return

    def save(self):
        """self to improv"""
        result = IMProvNode("Validate", None, Version = self.release)
        for item in self:
            result.addNode(item.save())
        return result

    def load(self, improvNode):
        """improv to self"""
        self.release = str(improvNode.attrs['Version'])
        query = IMProvQuery("Validate/Test")
        nodes = query(improvNode)
        for node in nodes:
            newTest = RelValTest()
            newTest.load(node)
            self.append(newTest)

        return

def writeSpec(filename, *relValInstances):
    """
    _writeSpec_

    Write out the RelValidation instances provided to the filename
    given. Will overwrite file if present

    """
    masterDoc = IMProvDoc("ReleaseValidation")
    for item in relValInstances:
        masterDoc.addNode(item.save())
    handle = open(filename, 'w')
    handle.write(masterDoc.makeDOMDocument().toprettyxml())
    handle.close()
    return




def readRelValSpec(url, topNode = "ReleaseValidation"):
    """
    _readRelValSpec_

    Open the given URL and read the contents, parse it and return a list of
    RelValidation objects

    """
    
    handler = IMProvHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.parse(url)
    mainNode = handler._ParentDoc

    result = []
    query = IMProvQuery("/%s/Validate" % topNode)
    nodes = query(mainNode)
    for node in nodes:
        relval = RelValidation("")
        relval.load(node)
        result.append(relval)
    return result
        

def getRelValSpecForVersion(url, *versions):
    """
    _getRelValSpecForVersion_
    
    """
    specs = readRelValSpec(url)
    result = None
    for item in specs:
        if item.release in versions:
            if result == None:
                result = item
            else:
                result.extend(item)
    return result

def listAllVersions(url):
    """
    _listAllVersions_

    
    
    """
    specs = readRelValSpec(url)
    result = []
    for item in specs:
        result.append(item.release)
    return result


if __name__ == '__main__':
    #val = RelValidation("CMSSW_0_7_0")
    #val.addTest("RelValSingleMuPlusPt100", 500, 'https://twiki.cern.ch/twiki/pub/CMS/Reco/single_mu_pt_100_positive.cfg')
    #val.addTest("RelValSingleMuMinusPt100", 500, 'https://twiki.cern.ch/twiki/pub/CMS/Reco/single_mu_pt_100_negative.cfg')
    #node = val.save()
    #print str(val.save())


    #val2 = RelValidation("")
    #val2.load(node)
    #print str(val2.save())

    print len(getRelValSpecForVersion('https://twiki.cern.ch/twiki/pub/CMS/Reco/RelVal070.xml', "CMSSW_0_7_0"))
