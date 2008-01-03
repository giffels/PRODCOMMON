#!/usr/bin/env python
"""
_CatalogParser_

Read an XML Pool Catalog and create a list of file information from it.

Eventually this should go away as this info gets inserted straight into the
job report, but for now we need to extract it from the catalog.

Note that this parser is not designed for large catalogs. If you are dealing
with large catalogs:
1. Consider using the CachedParser in CMSGLIDE
2. Consider who is repsonsible for creating large XML catalogs and devise a
way to cause them pain.

"""

from xml.sax.handler import ContentHandler
from xml.sax import make_parser



class FileEntry(dict):
    """
    _FileEntry_

    Dictionary container object for file entry 
    
    """
    def __init__(self):
        self.setdefault('GUID', None)
        self.setdefault('LFN',  None)
        self.setdefault('PFN', [])

    
    
class CatalogHandler(ContentHandler):
    """
    _CatalogHandler_

    Trip through the catalog and compile a list of file entries from
    it.
    """
    def __init__(self):
        ContentHandler.__init__(self)
        self.files = []
        self.currentFile = FileEntry()
    

    def startElement(self, name, attrs):
        """
        _startElement_

        """
        if name == "File":
            self.currentFile = FileEntry()
            # get ID from attrs
            idValue = attrs.get("ID", None)
            if idValue != None:
                self.currentFile['GUID'] = str(idValue)
            return
        if name == "pfn":
            pfnValue = attrs.get("name", None)
            if pfnValue != None:
                self.currentFile['PFN'].append(str(pfnValue))
            return
        if name == "lfn":
            lfnValue = attrs.get("name", None)
            if lfnValue != None:
                self.currentFile['LFN'] = str(lfnValue)
            return
                

    def endElement(self, name):
        """
        _endElement_

        """
        if name == "File":
            self.files.append(self.currentFile)
            self.currentFile == None

        return

    
    
def readCatalog(catalogXMLFile):
    """
    _readCatalog_

    Read the catalog and extract the list of files from it.
    Returns a list of FileEntry (dictionary) objects containing
    details of each file

    """

    #  //
    # // Hack out the goddammed InMemory DTD crap that POOL uses
    #//  so we can actually read the XML with the parser without
    #  //it blowing up.
    # //  
    #//  
    catalogContent = file(catalogXMLFile).read()
    catalogContent = catalogContent.replace(
        "<!DOCTYPE POOLFILECATALOG SYSTEM \"InMemory\">", "")
    
    handler = CatalogHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    
    parser.feed(catalogContent)
    result = handler.files
    del handler, parser, catalogContent
    return result

