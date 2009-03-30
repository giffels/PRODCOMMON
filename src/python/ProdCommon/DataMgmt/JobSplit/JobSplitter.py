#!/usr/bin/env python
"""
_JobSplitter_

Object that gets populated with DBS data and can be used to
split the fileblocks into jobs based on files or events

"""



class JobDefinition(dict):
    """
    _JobDefinition_

    Data object that contains details of content for a single
    job

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Fileblock", None)
        self.setdefault("SENames", [])
        self.setdefault("LFNS", [])
        self.setdefault("MaxEvents", None)
        self.setdefault("SkipEvents", None)
        self.setdefault("ParentLFNS", [])


class Fileblock(list):
    """
    _Fileblock_

    Object to represent a file block as a list of files

    """
    def __init__(self, name, *seNames):
        list.__init__(self)
        self.name = name
        self.seNames = list(seNames)



    def addFile(self, lfn, numEvents, parents = None):
        """
        _addFile_

        Add a file to this fileblock

        """
        self.append( (lfn, numEvents, parents) )
        return

    def isEmpty(self):
        """
        _isEmpty_

        If the file block has no SE Names or no files, then it
        is empty

        """
        if len(self.seNames) == 0:
            return True
        if len(self) == 0:
            return True
        return False

class JobSplitter:
    """
    _JobSplitter_

    Interface Object that gets populated with data by the DBS/DLS
    and is used to split the job into either files or events

    """
    def __init__(self, dataset):
        self.dataset = dataset
        self.fileblocks = {}


    def newFileblock(self, blockName, * seNames):
        """
        _newFileblock_

        Retrieve the Fileblock instance for the block name provided.
        If it doesnt exist, create a new block.
        If it does exist, return it

        """
        fileBlock = self.fileblocks.get(blockName, None)
        if fileBlock == None:
            fileBlock = Fileblock(blockName, * seNames)
            self.fileblocks[blockName] = fileBlock
        return fileBlock
                


    def listFileblocks(self):
        """
        _listFileblocks_

        return a list of fileblocks in this object

        """
        
        return self.fileblocks.keys()

    def totalFiles(self):
        """
        _totalFiles_

        Return a count of all files in all fileblocks.
        Useful for checking wether a dataset actually has files in it
        """
        result = 0
        for fileblock in self.fileblocks.values():
            for fileEntry in fileblock:
                result += fileEntry[1]

        return result

        

    def splitByFiles(self, fileblockName, filesPerJob = 1 ):
        """
        _splitByFiles_


        """
        result = []

        fileblock = self.fileblocks.get(fileblockName)

        currentJob = JobDefinition()
        currentJob['Fileblock'] = fileblock.name
        currentJob['SENames'] = fileblock.seNames
        
        counter = 0
        
        for file in fileblock:
            currentJob['LFNS'].append(file[0])
            if file[2] != None:
                currentJob['ParentLFNS'].extend(file[2])
            counter += 1
            if counter == filesPerJob:
                result.append(currentJob)
                currentJob = JobDefinition()
                currentJob['Fileblock'] = fileblock.name
                currentJob['SENames'] = fileblock.seNames
                counter = 0

        if counter > 0:
            #  //
            # // remainder
            #//
            result.append(currentJob)
        return result
            

    def splitByEvents(self, fileblockName, eventsPerJob):
        """
        _splitByEvents_

        
        """
        result = []

        fileblock = self.fileblocks.get(fileblockName)
        
        # block contains no files
        if not fileblock:
            return result

        carryOver = 0
        currentJob = JobDefinition()
        currentJob['Fileblock'] = fileblock.name
        currentJob['SENames'] = fileblock.seNames
        currentJob['MaxEvents'] = eventsPerJob
        currentJob['SkipEvents'] = 0
        lastLFN = None
        
        for file in fileblock:
            fileLFN = file[0]
            eventsInFile = file[1]
            lastLFN = fileLFN
            
            #  //
            # // Take into account offset.
            #//
            startEvent = eventsPerJob - carryOver

            #  //Edge Effect: 
            # // if start event is 0, we need to add this file
            #//  otherwise it will be picked up automatically
            if startEvent != 0:
                currentJob['LFNS'].append(fileLFN)            

            #  //
            # // Keep creating job defs while accumulator is within
            #//  file event range
            accumulator = startEvent
            while accumulator < eventsInFile:
                result.append(currentJob)
                currentJob = JobDefinition()
                currentJob['Fileblock'] = fileblock.name
                currentJob['SENames'] = fileblock.seNames
                currentJob['MaxEvents'] = eventsPerJob
                currentJob['LFNS'].append(fileLFN)
                currentJob['SkipEvents'] = accumulator
                
                accumulator += eventsPerJob

            #  //
            # // if there was a shortfall in the last job
            #//  pass it on to the next job
            accumulator -= eventsPerJob
            carryOver = eventsInFile - accumulator
            
        #  //
        # // remainder
        #//
        result.append(currentJob)
        return result
            
    
    
                        
