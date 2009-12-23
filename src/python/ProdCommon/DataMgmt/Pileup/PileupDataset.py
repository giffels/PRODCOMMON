#!/usr/bin/env python
"""

_PileupDataset_

Object that retrieves and contains a list of lfns for some pileup dataset.

Provides randomisation of access to the files, with two modes:

- No Overlap:  List of LFNs diminishes, and no pileup file is used twice per job
- Overlap:  Random selection of files from the list.


"""
import random
import logging

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader


class PileupDataset(dict):
    """
    _PileupDataset_

    List of files in a pileup dataset.

    Includes random access with and without overlap, and also a persistency
    mechanism to save state to
    a file if required
    
    """
    def __init__(self, dataset, dbsUrl):
        dict.__init__(self) # map of Blockname: list of files 
        self.sites = {}     # map of SE: blocknames
        self.blockSites = {} # map of Blockname: list of sites
        self.dataset = dataset 
        self.maxFilesPerJob = 100
        self.dbsUrl = dbsUrl
        self.targetModule = None # Mixing, DataMixing?


        
        

    def getPileupFiles(self, *sites):
        """
        _getPileupFiles_

        Get next randomised set of files. Returns list of filesPerJob lfns.
        If overlap is true, then the files are not removed from the list.
        If overlap is false, then the files are pruned from the list. When the list
        runs out of files, it will throw an exception

        """
        logging.debug("Max PU Files: %s" % self.maxFilesPerJob)
        possibleFiles = []
        matchedBlocks = {}
        #  //
        # // Selecting possible blocks
        #//
        if len(sites) > 0:
            logging.debug("Pileup Site Limit: %s" % str(sites))
            #  //
            # // Site limit
            #//
            filterSites = lambda x: x in sites
            for block in self.blockSites:
                filteredSites = filter(filterSites, self.blockSites[block])
                if filteredSites:
                    matchedBlocks[block] = filteredSites
            logging.debug("Matched Pileup Block: %s" % matchedBlocks.keys())
        else:
            #  //
            # // no site limit => all files
            #//
            logging.debug("No Site Limit on Pileup")
            matchedBlocks = self.blockSites

        #  //
        # // Select the files to return, start with something really simple.
        #//
        shuffleBlocks = matchedBlocks.keys()
        random.shuffle(shuffleBlocks)
        selectedBlock = shuffleBlocks[0] # Select one block randomly
        possibleFiles = self[selectedBlock]
        random.shuffle(possibleFiles)
        targetSites = matchedBlocks[selectedBlock]

        if len(possibleFiles) < self.maxFilesPerJob:
            return possibleFiles, targetSites

        return possibleFiles[0:self.maxFilesPerJob], targetSites
        



    def __call__(self):
        """
        _operator()_

        Load PU dataset information from DBS

        """
        
        
        reader = DBSReader(self.dbsUrl)
        blocks = reader.listFileBlocks(self.dataset, False)
        
        for block in blocks:
            #  //
            # // Populate locations
            #//
            locations = reader.listFileBlockLocation(block)
            if locations:
                self.blockSites[block] = locations
            for location in locations:
                if not self.sites.has_key(location):
                    self.sites[location] = set()
                self.sites[location].add(block)
            #  //
            # // Populate File list for block
            #//
            self[block] = reader.lfnsInBlock(block)

        return
        
        
        
    



def createPileupDatasets(workflowSpec):
    """
    _createPileupDatasets_

    Create PileupTools.PileupDataset instances for each of
    the pileup datasets in the workflowSpec.

    Return a dictionary mapping the payload node name to the
    list of PileupDataset instances

    """
    result = {}
    wfdbsUrl = workflowSpec.parameters.get("DBSURL", None)
    puDatasets = workflowSpec.pileupDatasets()
    for puDataset in puDatasets:
        dbsUrl =  puDataset.get("DBSURL", wfdbsUrl)
        pudInstance = PileupDataset(puDataset.name(), dbsUrl)
        if puDataset.has_key("TargetModule"):
            pudInstance.targetModule = puDataset['TargetModule']
        if puDataset.has_key("FilesPerJob"):
            pudInstance.maxFilesPerJob = int(puDataset['FilesPerJob'])
            pudInstance()
        
        result.setdefault(puDataset['NodeName'], []).append(pudInstance)

    return result



      
  




