#!/usr/bin/env python
"""
_RunResParser_

XML File opener and parser implementation used to extract the
RunResDB elements from XML files attached to a RunResDB
and create an IMProvNode structure containing the
RunRes information

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: RunResParser.py,v 1.1 2006/04/10 17:18:33 evansde Exp $"
__author__ = "evansde@fnal.gov"

from xml.sax.handler import ContentHandler
from xml.sax import make_parser

from IMProv.IMProvNode import IMProvNode

class RunResHandler(ContentHandler):
    """
    _RunResHandler_

    SAX Content Handler implementation to build an
    set of IMProvNode Trees from the RunResDB elements found in
    an XML Document
    
    """
    def __init__(self):
        ContentHandler.__init__(self)
        self.results = []
        self._ParentDoc = None
        self._NodeStack = []
        self._CharCache = ""
        self._Active = False
       
    def startElement(self, name, attrs):
        """
        _startElement_
        
        Override SAX startElement handler
        """
        if name == "RunResDB":
            self._Active = True
        if not self._Active:
            return
        if self._ParentDoc == None:
            self._ParentDoc = IMProvNode(str(name))
            self._NodeStack.append(self._ParentDoc)
            return
      
        self._CharCache = ""
        newnode = IMProvNode(str(name))
        for key, value in attrs.items():
            newnode.attrs[str(key)] = str(value)
            
        self._NodeStack[-1].addNode(newnode)
        self._NodeStack.append(newnode)
        return
        

    def endElement(self, name):
        """
        _endElement_

        Override SAX endElement handler
        """
        if not self._Active:
            return
        self._NodeStack[-1].chardata = str(self._CharCache.strip())
        self._NodeStack.pop()
        self._CharCache = ""
        if name == "RunResDB":
            self._Active = False
            self.results.append(self._ParentDoc)
            self._ParentDoc = None
            self._NodeStack = []
        return

    def characters(self, data):
        """
        _characters_

        Accumulate character data from an xml element
        """
        if not self._Active:
            return
        self._CharCache += data
        



def loadRunResURL(url):
    """
    _loadRunResURL_

    Open the url and parse the XML file there
    """
    handler = RunResHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.parse(url)
    return handler.results
