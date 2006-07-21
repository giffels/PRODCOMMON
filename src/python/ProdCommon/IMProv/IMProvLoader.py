#!/usr/bin/env python
# pylint: disable-msg=W0613,W0152
"""
_IMProvLoader_

Sax based parser for reading an IMProv XML file and
converting it into a tree of IMProvNodes

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: IMProvLoader.py,v 1.1 2006/04/10 17:01:34 evansde Exp $"


from xml.sax.handler import ContentHandler
from xml.sax import make_parser

from ProdCommon.IMProv.IMProvDoc import IMProvDoc
from ProdCommon.IMProv.IMProvNode import IMProvNode



class IMProvHandler(ContentHandler):
    """
    _IMProvHandler_

    SAX Content Handler implementation to build an
    IMProv Tree from an XML Document
    
    """
    def __init__(self):
        ContentHandler.__init__(self)
        self._ParentDoc = None
        self._NodeStack = []
        self._CharCache = ""
       
    def startElement(self, name, attrs):
        """
        _startElement_

        Override SAX startElement handler
        """
        if self._ParentDoc == None:
            self._ParentDoc = IMProvDoc(str(name))
            self._NodeStack.append(self._ParentDoc)
            return
        plainAttrs = {}
        self._CharCache = ""
        for key, value in attrs.items():
            plainAttrs[str(key)] = str(value)
        newnode = IMProvNode(str(name))
        for key, value in attrs.items():
            newnode.attrs[key] = value
        self._NodeStack[-1].addNode(newnode)
        self._NodeStack.append(newnode)
        return
        

    def endElement(self, name):
        """
        _endElement_

        Override SAX endElement handler
        """
        self._NodeStack[-1].chardata = str(self._CharCache.strip())
        self._NodeStack.pop()
        self._CharCache = ""

    def characters(self, data):
        """
        _characters_

        Accumulate character data from an xml element
        """
        self._CharCache += data
        
        

def loadIMProvFile(filename):
    """
    _loadIMProvFile_

    Load an XML Document into an IMProv Tree
    """
    handler = IMProvHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.parse(filename)
    return handler._ParentDoc








