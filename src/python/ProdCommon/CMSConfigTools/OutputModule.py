#!/usr/bin/env python
"""
_OutputModule_

Util for manipulating OutputModules within a PSet

"""

from ProdCommon.CMSConfigTools.Utilities import isQuoted

class OutputModule:
    """
    _OutputModule_

    Util for manipulating an OutputModule's settings within
    a cmsconfig object

    Instantiate with a Reference to the output module dictionary
    so that it can be manipulated.
    """
    def __init__(self, moduleName, moduleDictRef):
        self.name = moduleName
        self.data = moduleDictRef
        

    def catalog(self):
        """
        _catalog_

        Extract the output catalog, None if not present

        """
        tpl = self.data.get('catalog', None)
        if tpl != None:
            return tpl[2]
        return None

    def setCatalog(self, newCatalog):
        """
        _setCatalog_

        Set the catalog name

        """
        if not isQuoted(newCatalog):
            newCatalog = "\'%s\'" % newCatalog
        self.data['catalog'] = ('string', 'untracked', newCatalog)
        return

    
    def fileName(self):
        """
        _fileName_

        get the fileName parameter, None if not present

        """
        tpl = self.data.get('fileName', None)
        if tpl != None:
            return tpl[2]
        return None

    def setFileName(self, fname):
        """
        _setFileName_

        """
        if not isQuoted(fname):
            fname = "\'%s\'" % fname
        self.data['fileName'] = ('string', 'untracked', fname)

    def logicalFileName(self):
        """
        _logicalFileName_

        """
        tpl = self.data.get('logicalFileName', None)
        if tpl != None:
            return tpl[2]
        return None

    def setLogicalFileName(self, lfn):
        """
        _setLogicalFileName_

        """
        if not isQuoted(lfn):
            lfn = "\'%s\'" % lfn
        self.data['logicalFileName'] = ('string', 'untracked', lfn)

    def __str__(self):
        return str(self.data)
    


    def datasets(self):
        """
        _datasets_

        Extract a list of datasets from this module based on the
        contents of a dataset PSet if it exists.

        The return value is a list of dictionaries containing dataset
        information. 

        """
        result = []
        
        if self.data.has_key("dataset"):
            #  //
            # // new dataset {} approach
            #//
            content = self.data['dataset'][2]
            output = {}
            for key, value in content.items():
                output[key] = value[2].replace("\"", "")
            return [output]

        if self.data.has_key("datasets"):
            #  //
            # // old datasets { dataset1 } approach
            #//
            for dsEntry in self.data['datasets'][2].keys():
                content = self.data['datasets'][2][dsEntry][2]
                output = {}
                
                for key, value in content.items():
                    output[key] = value[2].replace("\"", "")
                result.append(output)
                
            return result
        
        return []
        
    def addDataset(self, entryName, **datasetParams):
        """
        _addDataset_

        Add Dataset information to this cfg file. Each key, value pair
        provided in dataset Params is added as an untracked string to
        the datasets PSet in a PSet named entryName

        """
        if not self.data.has_key("datasets"):
            self.data['datasets'] = ('PSet', 'untracked', {})

        if not self.data['datasets'][2].has_key(entryName):
            self.data['datasets'][2][entryName] = ('PSet', 'untracked', {})
        datasetDict = self.data['datasets'][2][entryName][2]

        for key, value in datasetParams.items():
            datasetDict[key] = ('string', 'untracked', '\"%s\"' % value)

        return
    
            
        

