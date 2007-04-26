#!/usr/bin/env python
"""
_OutputModule_

Util for manipulating OutputModules within a PSet

"""

import FWCore.ParameterSet.Types  as CfgTypes
from ProdCommon.CMSConfigTools.ConfigAPI.Utilities import _CfgNoneType


class OutputModule:
    """
    _OutputModule_

    Util for manipulating a Pool Output Module

    Instantiate with a Reference to the output module object
    so that it can be manipulated.
    """
    def __init__(self, moduleName, moduleRef):
        self.name = moduleName
        self.data = moduleRef
        

    def catalog(self):
        """
        _catalog_

        Extract the output catalog, None if not present

        """
        cfgType = getattr(self.data, "catalog", _CfgNoneType())
        return cfgType.value()

    def setCatalog(self, newCatalog):
        """
        _setCatalog_

        Set the catalog name

        """
        self.data.catalog = CfgTypes.untracked(CfgTypes.string(newCatalog))
        return

    
    def fileName(self):
        """
        _fileName_

        get the fileName parameter, None if not present

        """
        cfgType = getattr(self.data, "fileName", _CfgNoneType())
        return cfgType.value()
        
    def setFileName(self, fname):
        """
        _setFileName_

        """
        self.data.fileName = CfgTypes.untracked(CfgTypes.string(fname))

    def logicalFileName(self):
        """
        _logicalFileName_

        """
        cfgType = getattr(self.data, "logicalFileName", _CfgNoneType())
        return cfgType.value()

    def setLogicalFileName(self, lfn):
        """
        _setLogicalFileName_

        """
        self.data.logicalFileName = CfgTypes.untracked(CfgTypes.string(lfn))
        return
    
    def __str__(self):
        return self.data.dumpConfig()
    


    def dataset(self):
        """
        _dataset_

        Extract the dataset information from this module based on the
        contents of a dataset PSet if it exists.
        
        The return value is a list of dictionaries containing dataset
        information. 

        """

        datasetsPSet = getattr(self.data, "dataset", None)
        if datasetsPSet == None:
            return None

        result = {}
        for param in datasetsPSet.parameterNames_():
            result[param] = getattr(datasetsPSet, param).value()
            
        return result
        
        
    def addDataset(self, **datasetParams):
        """
        _addDataset_

        Add Dataset information to this cfg file. Each key, value pair
        provided in dataset Params is added as an untracked string to
        the datasets PSet in a PSet named entryName

        """
        datasetPSet = getattr(self.data, "dataset", None)
        if datasetPSet == None:
            self.data.dataset = CfgTypes.untracked(CfgTypes.PSet())
            datasetPSet = self.data.dataset
        
        
        for key, value in datasetParams.items():
            setattr(
                datasetPSet, key,
                CfgTypes.untracked(CfgTypes.string(str(value)))
                )
            
        return
    


    def moduleParameters(self):
        """
        _moduleParameters_

        Extract information from this object and return it
        as a plain dictionary

        """
        result = {}
        result['fileName'] = self.fileName()
        result['Catalog'] = self.catalog()
        result['logicalFileName'] = self.logicalFileName()

        dataset = self.dataset()
        if dataset != None:
            result.update(dataset)

        return result
        
        

