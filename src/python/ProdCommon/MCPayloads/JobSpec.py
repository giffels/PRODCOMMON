#!/usr/bin/env python
"""
_JobSpec_

Container class for a tree of JobSpecNodes representing a
concrete job definition

"""

import time

from ProdCommon.MCPayloads.JobSpecNode import JobSpecNode
from ProdCommon.MCPayloads.PayloadNode import getNodeByName
import ProdCommon.MCPayloads.DatasetTools as DatasetTools
import ProdCommon.MCPayloads.AppVersionTools as AppVersionTools

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvLoader import loadIMProvString
from IMProv.IMProvQuery import IMProvQuery



class BulkSpecs(dict):
    """
    _BulkSpecs_

    Object for representing a Bulk JobSpec.

    basically a map of job name to job spec file to allow
    a single JobSpec to represent many jobs.

    Kind of clunky, but useful for a first pass

    """
    def __init__(self):
        dict.__init__(self)

    def addJobSpec(self, jobSpecId, jobSpecFile):
        """
        _addJobSpec_

        Can also just use dict assignment
        """
        self[jobSpecId] = jobSpecFile
        return

    def save(self):
        """
        _save_

        return IMProv structure of self

        """
        result = IMProvNode("BulkSpecs")
        for key, value in self.items():
            result.addNode(IMProvNode("BulkSpec", value, ID = key))
        return result

    def load(self, improvNode):
        """
        _load_

        Load from IMProvNode structure & populate self

        """
        nodesQ = IMProvQuery("/BulkSpecs/BulkSpec")
        nodes = nodesQ(improvNode)
        for node in nodes:
            id = node.attrs.get("ID", None)
            if id == None:
                continue
            self[str(id)] = str(node.chardata)
        return
    

    def isPopulated(self):
        """
        _isPopulated_

        True of False wether this contains entries
        
        """
        return len(self.keys()) > 0
    
    


class JobSpec:
    """
    _JobSpec_

    JobSpecNode tree toplevel container

    """
    def __init__(self):
        self.payload = JobSpecNode()
        self.parameters = {}
        self.parameters.setdefault("JobName", "Job-%s" % time.time())
        self.parameters.setdefault("JobType", "Processing")
        self.siteWhitelist = []
        self.siteBlacklist = []

        #  //
        # // Used for PM/PA interaction, not normal jobs
        #//
        self.associatedFiles = {}

        #  //
        # // Used for Bulk Specs, ignored when empty
        #//
        self.bulkSpecs = BulkSpecs()
        
        
    def addWhitelistSite(self, sitename):
        """
        _addWhitelistSite_

        Add a site to the whitelist

        """
        if sitename not in self.siteWhitelist:
            self.siteWhitelist.append(sitename)
        return
    
            
    def addBlacklistSite(self, sitename):
        """
        _addBlacklistSite_

        Add a site to the blacklist

        """
        if sitename not in self.siteBlacklist:
            self.siteBlacklist.append(sitename)
        return
    
            
        
    def setJobName(self, jobName):
        """
        set the name for this job

        """
        self.parameters['JobName'] = jobName
        updateJobName(self.payload, jobName)
        return

    def setJobType(self, jobType):
        """
        set the job type for this job

        """
        self.parameters['JobType'] = jobType
        updateJobType(self.payload, jobType)
        return

    def isBulkSpec(self):
        """
        _isBulkSpec_

        True or False wether this is a bulk spec or not

        """
        return self.bulkSpecs.isPopulated()


    def addAssociatedFiles(self, listName, *fileData):
        """
        _addAssociatedFiles_

        Add a set of associated files to this JobSpec

        """
        if not self.associatedFiles.has_key(listName):
            self.associatedFiles[listName] = []

        knownLFNs = [ x['LFN'] for x in self.associatedFiles[listName] ]

        for fData in fileData:
            lfn = fData['LFN']
            if lfn in knownLFNs:
                msg = "Duplicate LFN in list %s\n" % listName
                msg += " %s already exists\n " % lfn
                raise RuntimeError, msg

            self.associatedFiles[listName].append(fData)
        return
    
            

    def findNode(self, nodeName):
        """
        _findNode_

        Find the JobSpec node with the name provided in this instance

        """
        return getNodeByName(self.payload, nodeName)
    
    def makeIMProv(self):
        """
        _makeIMProv_

        Serialise the WorkflowSpec instance into an XML IMProv structure

        """
        node = IMProvNode("JobSpec")
        for key, val in self.parameters.items():
            paramNode = IMProvNode("Parameter", str(val), Name = str(key))
            node.addNode(paramNode)
        whitelist = IMProvNode("SiteWhitelist")
        blacklist = IMProvNode("SiteBlacklist")
        node.addNode(whitelist)
        node.addNode(blacklist)
        for site in self.siteWhitelist:
            whitelist.addNode(
                IMProvNode("Site", None, Name = site)
                )
        for site in self.siteBlacklist:
            blacklist.addNode(
                IMProvNode("Site", None, Name = site)
                )

        if len(self.associatedFiles.keys()) > 0:
            assocFiles = IMProvNode("AssociatedFiles")
            for key, val in self.associatedFiles.items():
                assocList = IMProvNode("AssocFileList", None, Name = key)
                assocFiles.addNode(assocList)
                for fileEntry in val:
                    fileNode = IMProvNode("AssocFile", fileEntry['LFN'])
                    for fileAttr, fileVal in fileEntry.items():
                        if fileAttr == "LFN":
                            continue
                        fileNode.attrs[fileAttr] = str(fileVal)
                    assocList.addNode(fileNode)
            node.addNode(assocFiles)

        if self.isBulkSpec():
            node.addNode(self.bulkSpecs.save())
        
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
   
    def saveString(self):
        """
        _saveString_

        Save this object to an XML string

        """
        improv = self.makeIMProv()
        return str(improv.makeDOMElement().toprettyxml())

    def loadFromNode(self, improvNode):

        paramQ = IMProvQuery("/JobSpec/Parameter")
        payloadQ = IMProvQuery("/JobSpec/Payload/JobSpecNode")
        whitelistQ = IMProvQuery("/JobSpec/SiteWhitelist/Site")
        blacklistQ = IMProvQuery("/JobSpec/SiteBlacklist/Site")
        assocFileQ = IMProvQuery("/JobSpec/AssociatedFiles/AssocFileList")
        bulkSpecQ = IMProvQuery("/JobSpec/BulkSpecs")

        #  //
        # // Extract Params
        #//
        paramNodes = paramQ(improvNode)
        for item in paramNodes:
            paramName = item.attrs.get("Name", None)
            if paramName == None:
                continue
            paramValue = str(item.chardata)
            self.parameters[str(paramName)] = paramValue

        #  //
        # // Extract site lists
        #//
        whiteNodes = whitelistQ(improvNode)
        for wnode in whiteNodes:
            value = wnode.attrs.get("Name", None)
            if value != None:
                self.siteWhitelist.append(str(value))
        blackNodes = blacklistQ(improvNode)
        for bnode in blackNodes:
            value = bnode.attrs.get("Name", None)
            if value != None:
                self.siteBlacklist.append(str(value))

        #  //
        # // Extract Associated Files
        #//
        assocFiles = assocFileQ(improvNode)
        if len(assocFiles) > 0:
            for assocFileList in assocFiles:
                assocListName = str(assocFileList.attrs['Name'])
                assocList = []
                for aFile in assocFileList.children:
                    fileEntry = {
                        "LFN" : str(aFile.chardata),
                        }
                    for attrName, attrVal in aFile.attrs.items():
                        fileEntry[str(attrName)] = str(attrVal)
                    assocList.append(fileEntry)
                self.addAssociatedFiles(assocListName, *assocList)

        bulkNodes = bulkSpecQ(improvNode)
        if len(bulkNodes) > 0:
            self.bulkSpecs.load(bulkNodes[-1])
        
        #  //
        # // Extract Payload Nodes
        #//
        payload = payloadQ(improvNode)[0]
        self.payload = JobSpecNode()
        self.payload.populate(payload)
        return


    def load(self, filename):
        """
        _load_

        Load a saved JobSpec object and install its information
        into this instance

        """
        node = loadIMProvFile(filename)
        self.loadFromNode(node)
        return

    def loadString(self, xmlString):
        """
        _load_

        Load a saved JobSpec from a string

        """
        node = loadIMProvString(xmlString)
        self.loadFromNode(node)
        return


    #  //
    # // Accessor methods for retrieving dataset information
    #//
    def outputDatasets(self):
        """
        _outputDatasets_

        returns a list of MCPayload.DatasetInfo objects (essentially
        just dictionaries) containing all of the output datasets
        in all nodes of this JobSpec including details of output modules
        (Catalog etc) if there is a matching output module for each dataset

        Note that this method overrides the outputDatsets method in
        PayloadNode. It returns the same information as that method but
        augmented with output module details from the configuration

        """
        return DatasetTools.getOutputDatasetDetailsFromTree(self.payload)

    
    def listApplicationVersions(self):
        """
        _listApplicationVersions_

        Traverse all of this JobSpec instances nodes and retrieve the
        Application versions from each node and return a list of all
        of the unique Version strings

        Eg if there are three nodes all using CMSSW_X_X_X, then there
        will be a single entry for that version
        
        """
        return AppVersionTools.getApplicationVersions(self.payload)
        

def updateJobName(jobSpecNode, jobNameVal):
    """
    _updateJobName_

    Propagate JobName down to all JobSpec nodes in the tree

    """
    jobSpecNode.jobName = jobNameVal
    for child in jobSpecNode.children:
        updateJobName(child, jobNameVal)
    return

def updateJobType(jobSpecNode, jobTypeVal):
    """
    _updateJobType_

    Propagate JobType to all JobSpec nodes in tree

    """
    jobSpecNode.jobType = jobTypeVal
    for child in jobSpecNode.children:
        updateJobType(child, jobTypeVal)
    return
