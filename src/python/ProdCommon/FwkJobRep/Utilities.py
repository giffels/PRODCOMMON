#!/usr/bin/env python
"""
_Utilities_

Util objects for working with job report objects

"""



class FileParentageSorter:
    """
    _FileParentageSorter_

    With parent remapping in redneck style jobs where one output module needs to look like the parent of
    another module, the files need to be sorted so that parents are inserted before children.

    This sorting class will sort the files in a job report instance and return them as a list ordered by
    parent dependencies

    """
    def __init__(self):
        self.result = []
        self.dependencies = {}




    def __call__(self, jobReport):
        """
        _operator(jobReport)_

        return a sorted list of files in the job report instance provided

        """
        self.result = []
        self.dependencies = {}
        [ self.append(f) for f in jobReport.files ]
        return self.sort()



    def append(self, element):
        """

        Add a file to the sorter, parentage is examined and added to the dependency map

        """
        name = element['LFN']
        parents = [ x['LFN'] for x in element.inputFiles ]
        if parents != []:
            self.dependencies[name] = parents

        self.result.append(element)

    def names(self):
        """
        _names_

        Current list of LFNs

        """
        return [ x['LFN'] for x in self.results ]

    def getEntry(self, name):
        """
        _getEntry_

        Get the file entry based on the LFN

        """
        for x in self.result:
            if x['LFN'] == name:
                return x
        return None

    def maxEntry(self, entryList):
        """
        _maxEntry_

        get the entry with the biggest index in the list (IE the last one in the list order)

        """
        maxIndex = max( [ self.result.index(x) for x in entryList ] )
        return self.result[maxIndex]


    def sort(self):
        """
        _sort_

        generate the sorted list of file objects, reshuffling file entries with child dependencies
        to ensure that they are behind the files they depend on

        """
        allnames = [ x['LFN'] for x in self.result ]
        for name, deps in self.dependencies.items():
            cur = self.getEntry(name)
            for dep in deps:
                if dep not in allnames:
                    continue
                dep = self.getEntry(dep)
                depPosition = self.result.index(dep)
                currPosition = self.result.index(cur)
                if depPosition > currPosition:
                    self.result.pop(depPosition)
                    self.result.insert(currPosition, dep)

        return self.result

