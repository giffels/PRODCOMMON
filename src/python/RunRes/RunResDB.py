#!/usr/bin/env python
"""
_RunResDB_

Database like interface to a collection of XML files that contain
RunRes XML Elements.

Readable XML files can be attached to the DB instance and will be loaded
and searched for a run-res node, which is loaded as an IMProvNode
and added to the DB.

RunResDB files can also be pointed at in the RunRes XML Elements
that provide a URL to the xml file which will be read and opened

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: RunResDB.py,v 1.1 2006/04/10 17:18:33 evansde Exp $"
__author__ = "evansde@fnal.gov"

import os.path

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery

from RunRes.RunResParser import loadRunResURL
from RunRes.RunResError import RunResError

class RunResDB(dict):
    """
    _RunResDB_

    Database like frontend for a collection of RunRes XML files
    to be attached to.
    Dictionary based interface, each DB file is stored as a key value
    pair mapping file URL : IMProvNode containing the RunResDB element
    from the XML File after loading.


    """
    def __init__(self):
        dict.__init__(self)


    def addInputURL(self, xmlFileURL):
        """
        _addInputURL_

        Add an input URL to the DB via URL
        The URL must point to an XML file that contains a
        <RunResDB/> XML node structure.
        
        """
        self[xmlFileURL] = None
        return


    
    def write(self, filename):
        """
        _write_

        Write out an XML file containing all of the RunResDB nodes
        currently in memory, removing any URL references embedded in them
        to avoid loading duplicates when the file produced is read

        """
        masterNode = IMProvDoc("RunResOutput")
        nodelist = []
        for key in self.keys():
            if self[key] == None:
                continue
            nodelist.extend(self[key])

            
        for node in nodelist:
            if "RunResDBURL" in node.keys():
                del node["RunResDBURL"]
                for i in range(0, len(node.children)):
                    item = node.children[i]
                    if item.name == "RunResDBURL":
                        node.children.pop(i)
                        
            masterNode.addNode(node)
            
        handle = open(filename, 'w')
        handle.write(masterNode.makeDOMDocument().toprettyxml())
        handle.close()
        return
            
            

    def load(self):
        """
        _load_

        For each URL added to this DB Instance, open the URL
        and extract the RunResDB elements from it.
        Convert that element into an IMProvNode instance
        containing its data and then add it to the DB structure

        """
        for url in self.keys():
            self._LoadURL(url)

        return

    def reset(self):
        """
        _reset_

        delete all the in-memory DB elements and set the DB values
        to None
        
        """
        for url in self.keys():
            del self[url]
            self[url] = None
        return

    def reload(self):
        """
        _reload_

        Calls reset to dump all data currently in memory and then load
        to reload all the URLs currently attached
        """
        self.reset()
        self.load()
        return
        

    def _LoadURL(self, url):
        """
        _LoadURL_

        Internal method to load up a URL, open it and add it to the
        RunResDB. This method will not open a URL that is already
        in the DB URL list and has a non-None value assigned to it
        to avoid loop references where two DB files may reference each other
        """
        #  //
        # // Ignore the url if this DB already knows about it
        #//
        if self.has_key(url):
            if self[url] != None:
                return
            
        #  //
        # // Now open the file and parse it
        #//  TODO: Error handling & propagation
        value = []
        #  //
        # // Returns a list of IMProvNodes containing RunResDB
        #//  nodes and their contentsm these are added to the keys
        data = loadRunResURL(url)
        value.extend(data)
        self[url] = value
        #  //
        # // Now search the DBs for extra URLs that point to other 
        #//  DB component files and attempt to load them if they are
        #  //Not already known about
        # //
        #//
        for entry in data:
            #  //
            # // Extract all URLs for referenced files
            #//
            rrURLQuery = IMProvQuery(
                "RunResDB/RunResDBURL[attribute(\"URL\")]"
                )
            urlRefs = rrURLQuery(entry)
            for ref in urlRefs:
                ref = os.path.expandvars(ref)
                #  //
                # // If this DB doesnt already know about the file,
                #//  load it up
                if self.has_key(ref):
                    continue
                self[ref] = None
                self._LoadURL(ref)
        return
    
    def dump(self, handle):
        """
        _dump_

        Dump out all RunResDB information to the file handle provided
        """
        for url in self.keys():
            handle.write("URL: %s\n" % url)
            if self[url] == None:
                handle.write("No Entry Found\n")
                continue
            for item in self[url]:
                handle.write(str(item))
                handle.write("\n")
        return
    
    


    def query(self, queryExpr):
        """
        _query_

        Evaluate the query to on all of the RunResDB objects
        in memory and return the results of the query.
        
        """
        result = []
        for key in self.keys():
            if self[key] == None:
                continue
            for entry in self[key]:
                query = IMProvQuery(queryExpr)
                tmpResults = query(entry)
                result.extend(tmpResults)
                

        return result
    
        
        

    def addRunResNode(self, keyname, improvTree):
        """
        _addNodeTree_

        Add a RunResDB improvTree provided
        to this  DB instance for in memory queries.
        
        The node must be a toplevel RunResDB node
        
        """
        if improvTree.name != "RunResDB":
            msg = "Node added to RunResDB instance is not a RunResDB node\n"
            msg += "The node provided must be a toplevel RunResDB node\n"
            raise RunResError(msg, ClassInstance = self, BadNode = improvTree)

        self[keyname] = improvTree
        return
    
        


        
