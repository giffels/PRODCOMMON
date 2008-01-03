#!/usr/bin/env python
"""
_FileInfo_

Container object for file information.
Contains information about a single file as a dictionary

"""

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery

from ProdCommon.MCPayloads.DatasetInfo import DatasetInfo

class FileInfo(dict):
    """
    _FileInfo_

    Dictionary based container for holding details about a
    file.
    Majority of keys are key:string single value, however a
    few need to be list based

    """
    
    def __init__(self):
        dict.__init__(self)
        self.setdefault("LFN", None)
        self.setdefault("PFN", None)
        self.setdefault("GUID", None)
        self.setdefault("Size", None)
        self.setdefault("TotalEvents", None)
        self.setdefault("EventsRead", None)
        self.setdefault("SEName", None)
        self.setdefault("ModuleLabel", None)
        self.setdefault("Catalog", None)
        self.setdefault("OutputModuleClass", None)
        

        #  //
        # // Is this an input or output file?
        #//
        self.isInput = False

        #  //
        # //  open/closed state
        #//
        self.state = "closed"

        #  //
        # // Output files is a list of input files which contain
        #//  the LFN and PFN of all contributing inputs
        self.inputFiles = []

        #  //
        # // List of Branch names
        #//
        self.branches = []

        #  //
        # // List of Runs
        #//
        self.runs = []

        #  //
        # // Dataset is a dictionary and will have the same key
        #//  structure as the MCPayloads.DatasetInfo object
        self.dataset = []
        
        #  //
        # // Checksums include a flag indicating which kind of 
        #//  checksum alg was used.
        self.checksums = {}

        #  //
        # // Lumi section information
        #//
        self.lumisections = []
        
        
    def addInputFile(self, pfn, lfn):
        """
        _addInputFile_

        Add an input file LFN and event range used as input to produce the
        file described by this instance.

        NOTE: May need to allow multiple ranges per file later on for skimming
        etc. However care must be taken to ensure we dont end up with event
        lists, since these will be potentially huge.

        """
        self.inputFiles.append({"PFN" : pfn,
                                "LFN" : lfn})
        return

    def newDataset(self):
        """
        _newDataset_

        Add a new dataset that this file is associated with and return
        the dictionary to be populated

        """
        newDS = DatasetInfo()
        self.dataset.append(newDS)
        return newDS
        
    def addChecksum(self, algorithm, value):
        """
        _addChecksum_

        Add a Checksum to this file. Eg:
        "cksum", 12345657

        """
        self.checksums[algorithm] = value
        return

    def addLumiSection(self, lumiSectionNumber,
                       runNumber):
        """
        _addLumiSection_

        Associate this file with a Lumi section.

        If the run number is not in the list of runs, then add it

        """
        lumiSect = {
            "LumiSectionNumber" : lumiSectionNumber,
            "StartEventNumber" : None,
            "EndEventNumber" : None,
            "LumiStartTime" : None,
            "LumiEndTime" : None,
            "RunNumber" : runNumber,
            }
        
        if (runNumber != None) and (runNumber not in self.runs):
            self.runs.append(runNumber)
        self.lumisections.append(lumiSect)
        return lumiSect

    
    
    def save(self):
        """
        _save_

        Return an improvNode structure containing details
        of this object so it can be saved to a file

        """
        if self.isInput == True:
            improvNode = IMProvNode("InputFile")
        if self.isInput == False:
            improvNode = IMProvNode("File")
        #  //
        # // General keys
        #//
        for key, val in self.items():
            if val == None:
                continue
            node = IMProvNode(str(key), str(val))
            improvNode.addNode(node)

        #  //
        # // Checksums
        #//
        for key, val in self.checksums.items():
            improvNode.addNode(IMProvNode("Checksum", val, Algorithm = key) )
            
        #  //
        # // State
        #//
        improvNode.addNode(IMProvNode("State", None, Value = self.state))

        #  //
        # // Inputs
        #//
        if not self.isInput:
            inputs = IMProvNode("Inputs")
            improvNode.addNode(inputs)
            for inputFile in self.inputFiles:
                inpNode = IMProvNode("Input")
                for key, value in inputFile.items():
                    inpNode.addNode(IMProvNode(key, value))
                inputs.addNode(inpNode)

    
        #  //
        # // Runs
        #//
        runs = IMProvNode("Runs")
        improvNode.addNode(runs)
        for run in self.runs:
            runs.addNode(IMProvNode("Run", run))

        #  //
        # // Dataset info
        #//
        if not self.isInput:
            datasets = IMProvNode("Datasets")
            improvNode.addNode(datasets)
            for datasetEntry in self.dataset:
                datasets.addNode(datasetEntry.save())
                
        #  //
        # // Branches
        #//
        branches = IMProvNode("Branches")
        improvNode.addNode(branches)
        for branch in self.branches:
            branches.addNode(IMProvNode("Branch", branch))


        #  //
        # // Lumi Sections
        #//
        lumi = IMProvNode("LumiSections")
        improvNode.addNode(lumi)
        for lumiSect in self.lumisections:
            node = IMProvNode("LumiSection")
            [ node.addNode(
                IMProvNode(str(x[0]), None, Value = str(x[1]) ))
              for x in lumiSect.items() if x[1] != None ]
            lumi.addNode(node)
            
        return improvNode

    
    def load(self, improvNode):
        """
        _load_

        Populate this object from the improvNode provided

        """
        #  //
        # // Input or Output?
        #//
        queryBase = improvNode.name
        if queryBase == "InputFile":
            self.isInput = True
        else:
            self.isInput = False
        #  //
        # // Parameters
        #//
        paramQ = IMProvQuery("/%s/*" % queryBase)
        for paramNode in paramQ(improvNode):
            if paramNode.name not in self.keys():
                continue
            self[paramNode.name] = paramNode.chardata

        
        #  //
        # // State
        #//
        stateQ = IMProvQuery("/%s/State[attribute(\"Value\")]" % queryBase)
        self.state = stateQ(improvNode)[-1]

        
        
        #  //
        # // Checksums
        #//
        cksumQ = IMProvQuery("/%s/Checksum" % queryBase)
        for cksum in cksumQ(improvNode):
            algo = cksum.attrs.get('Algorithm', None)
            if algo == None: continue
            self.addChecksum(str(algo), str(cksum.chardata))
            

        #  //
        # // Inputs
        #//
        inputFileQ = IMProvQuery("/%s/Inputs/Input" % queryBase)
        for inputFile in inputFileQ(improvNode):
            lfn = IMProvQuery("/Input/LFN[text()]")(inputFile)[-1]
            pfn = IMProvQuery("/Input/PFN[text()]")(inputFile)[-1]
            self.addInputFile(pfn, lfn)

        #  //
        # // Datasets
        #//
        datasetQ = IMProvQuery("/%s/Datasets/DatasetInfo" % queryBase)
        for dataset in datasetQ(improvNode):
            newDataset = self.newDataset()
            newDataset.load(dataset)

        #  //
        # // Branches
        #//
        branchQ = IMProvQuery("/%s/Branches/Branch[text()]" % queryBase)
        for branch in branchQ(improvNode):
            self.branches.append(str(branch))

        #  //
        # // Runs
        #//
        runQ = IMProvQuery("/%s/Runs/Run[text()]" % queryBase)
        for run in runQ(improvNode):
            self.runs.append(int(run))
            
        #  //
        # // Lumi Sections
        #//
        lumiQ = IMProvQuery("/%s/LumiSections/LumiSection" % queryBase)
        for lumiSect in lumiQ(improvNode):
            newLumi = self.addLumiSection(None, None)
            
            [ newLumi.__setitem__(x.name, x.attrs['Value'])
              for x in  lumiSect.children ]

        return
