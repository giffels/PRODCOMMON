#!/usr/bin/env python
"""
_DropMaker_

Generate the XML file for injecting data into PhEDEx

"""

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader


class XMLFileblock(list):
    """
    _XMLFileblock_

    Object representing a fileblock for conversion to XML

    """
    def __init__(self, fileblockName, isOpen = "y"):
        list.__init__(self)
        self.fileblockName = fileblockName
        self.isOpen = isOpen

    def addFile(self, lfn, checksum, size):
        """
        _addFile_

        Add a file to this fileblock

        """
        self.append(
            ( lfn, checksum, size, )
            )
        return

    def save(self):
        """
        _save_

        Serialise this to XML compatible with PhEDEx injection

        """
        result = IMProvNode("block")
        result.attrs['name'] = self.fileblockName
        result.attrs['is-open'] = self.isOpen
        for lfn, checksums, size in self:
            # checksums is a comma separated list of key:value pair
            checksum = ",".join(["%s:%s" % (x, y) for x, y \
                                 in checksums.items() \
                                 if y not in (None, '')])
            result.addNode(
                IMProvNode("file", None,
                           lfn = lfn,
                           checksum = checksum,
                           size = size)
                )
        return result

class XMLInjectionSpec:
    """
    _XMLInjectionSpec_

    <dbs name='DBSNameHere'>

    <dataset name='DatasetNameHere' is-open='boolean' is-transient='boolean'>
    <block name='fileblockname' is-open='boolean'>
    <file lfn='lfn1Here' checksum='cksum:0,cksum2:0' size ='fileSize1Here'/>
    <file lfn='lfn2Here' checksum='cksum:0' size ='fileSize2Here'/> </block>
    </dataset>
    </dbs> 
    """
    def __init__(self, dbs, 
                 datasetName, 
                 datasetOpen = "y",
                 datasetTransient = "n" ):
        self.dbs = dbs
        #  //
        # // dataset attributes
        #//
        self.datasetName = datasetName
        self.datasetIsOpen = datasetOpen
        self.datasetIsTransient = datasetTransient

        #  //
        # // Fileblocks
        #//
        self.fileblocks = {}


    def getFileblock(self, fileblockName, isOpen = "y"):
        """
        _getFileblock_

        Add a new fileblock with name provided if not present, if it exists,
        return it

        """
        if self.fileblocks.has_key(fileblockName):
            return self.fileblocks[fileblockName]
        
        newFileblock = XMLFileblock(fileblockName, isOpen)
        self.fileblocks[fileblockName] = newFileblock
        return newFileblock

    def save(self):
        """
        _save_

        serialise object into PhEDEx injection XML format

        """
        result = IMProvNode("dbs")
        result.attrs['name'] = self.dbs
        result.attrs['dls'] = 'dbs'
        dataset = IMProvNode("dataset")
        dataset.attrs['name'] = self.datasetName
        dataset.attrs['is-open'] = self.datasetIsOpen
        dataset.attrs['is-transient'] = self.datasetIsTransient

        
        result.addNode(dataset)

        for block in self.fileblocks.values():
            dataset.addNode(block.save())

        return result

    def write(self, filename):
        """
        _write_

        Write to file using name provided

        """
        handle = open(filename, 'w')
        improv = self.save()
        handle.write(improv.makeDOMElement().toprettyxml())
        handle.close()
        return
        

        
        


def makePhEDExDrop(dbsUrl, datasetPath, *blockNames):
    """
    _makePhEDExDrop_

    Given a DBS2 Url, dataset name and list of blockNames,
    generate an XML structure for injection

    """
    spec = XMLInjectionSpec(dbsUrl, 
                            datasetPath)


    reader = DBSReader(dbsUrl)

    for block in blockNames:
        blockContent = reader.getFileBlock(block)
        isOpen = reader.blockIsOpen(block)
        
        if isOpen:
            xmlBlock = spec.getFileblock(block, "y")
        else:
            xmlBlock = spec.getFileblock(block, "n")

        for x in blockContent[block]['Files']:
            checksums = {'cksum' : x['Checksum']}
            if x.get('Adler32') not in (None, ''):
                checksums['adler32'] = x['Adler32'] 
            xmlBlock.addFile(x['LogicalFileName'], checksums, x['FileSize'])

    improv = spec.save()
    xmlString = improv.makeDOMElement().toprettyxml()
    return xmlString
  
