#!/usr/bin/env python

"""
_IMProvAddEntry_

Allows to load an xml from file and to add sequential entries
(each one under the root tag)

note: this depends on pyXml package for xml.dom.* packages
"""

import xml.dom.minidom
import xml.dom.ext
import os

class Event:
    """
    _Event_

    Represent the information to be published in the xml file (a NODE)
    """
    
    #--------------------------------------------------------------------
    def __init__(self, maintag = "Event"):
        self.fields      = {}
        self.eventreport = maintag
        self.report      = None
        
        self.doc = xml.dom.minidom.Document()
        
    #--------------------------------------------------------------------
    def initialize(self, values):
        """
        _initialize_ 
        """
        self.fields = values

        eventrep = self.doc.createElement(self.eventreport)
        for key, value in self.fields.iteritems():
            if value is not None:
                eventrep.setAttribute(str(key), str(value))

        self.report = eventrep
        return self

    #--------------------------------------------------------------------
    def getDoc(self):
        """
        _getDoc_
        """
        return self.report

    #--------------------------------------------------------------------
    def toXml(self):
        """
        _toXml_
        """
        return self.report.toxml()

    #--------------------------------------------------------------------
    def dump(self, filepath):
        """
        _dump_
        """
        import pickle
        output = open( filepath, 'w')
        pickle.dump( self, output, -1 )
        output.close()

##-----------------------------------------------------------------------------

class IMProvAddEntry:
    """
    _IMProvAddEntry_

    """
    
    #--------------------------------------------------------------------
    def __init__(self, rootname ):
        self.rootname       = rootname
        self.doc            = xml.dom.minidom.Document()
        self.root           = self.doc.createElement( self.rootname )
        self.init           = False

    #--------------------------------------------------------------------
    def initialize(self):
        """
        _initialize_
        """
        self.doc.appendChild(self.root)
        self.init = True

    #--------------------------------------------------------------------
    def addNode(self, node):
        """
        _addNode_
        """
        self.root.appendChild( node.getDoc() )

    #--------------------------------------------------------------------
    def getFirstElementOf(self, tag):
        """
        _getFirstElementOf_
        """
        tagdiction = {}
        Events = self.doc.getElementsByTagName( tag )
        for eve in Events:
            return eve
        return None

    #--------------------------------------------------------------------
    def replaceEntry(self, nodeold, newtag, newdict):
        """
        _replaceEntry_
        """
        if nodeold is None:
            raise Exception("Cannot replace an None tag!")
        doctemp = xml.dom.minidom.Document()
        temp = doctemp.createElement( newtag )
        for key, value in newdict.iteritems():
            temp.setAttribute(key, value)
        self.root.replaceChild(temp, nodeold)

    #--------------------------------------------------------------------
    def toXml(self):
        """
        _toXml_
        """
        return self.doc.toxml()

    #--------------------------------------------------------------------
    def printMe(self):
        """
        _printMe_
        """
        if not self.init:
            raise RuntimeError, "Module IMProvAddEntry is not initialized." + \
                                "Call IMProvAddEntry.initialize(...) first"

        xml.dom.ext.PrettyPrint(self.doc)

    #--------------------------------------------------------------------
    def toFile(self, filename):
        """
        _toFile_
        """
        if not self.init:
            raise RuntimeError, "Module IMProvAddEntry is not initialized." + \
                                "Call IMProvAddEntry.initialize(...) first"

        filenametmp = filename + ".tmp"
        filet = open(filenametmp, 'w')
        xml.dom.ext.PrettyPrint(self.doc, filet)
        filet.close()
        commandrename = "mv "+str(filenametmp)+" "+str(filename)+";"
        # this should be an atomic operation thread-safe and multiprocess-safe
        os.popen( commandrename )

    #--------------------------------------------------------------------
    def fromFile(self, filename):
        """
        _fromFile_
        """
        if not os.path.exists(filename):
            errmeg = "Cannot open file [" + filename + "] for reading." + \
                     " File is not there."
            raise RuntimeError, errmeg

        self.doc = xml.dom.minidom.parse( filename )

        element = self.doc.getElementsByTagName( self.rootname )
        if len(element) == 0:
            errmsg = "Cannot find root node with name '" + self.rootname + \
                     "' in the xml document ["+filename+"]"
            raise RuntimeError, errmsg

        self.root = element[0]
        print element
        
        self.init = True
        

if __name__ == "__main__":
    LOADXML = IMProvAddEntry("InternalLogInfo")
    #LOADXML.fromFile('pollingThread_1.xml')
    LOADXML.fromFile('test.xml')
    EVE = Event("Event")
    import time
    INFO = { \
             "date":   time.time(), \
             "ev":     "prova", \
             "txt":    "Prima prova xml", \
             "reason": "develop logging info", \
             "code":   "00", \
             "time":   "0.0", \
             "author": "pipposfdsfds" ,\
             "exc":    "Exception: asldkjalskdjaklsjdak\n"
           }
    EVE.initialize(INFO)
    LOADXML.addNode(EVE)
    print "adding " + str(EVE.getDoc())
    LOADXML.printMe()
    LOADXML.toXml()
    LOADXML.toFile('test.xml')

