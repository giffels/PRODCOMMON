#!/usr/bin/env python
"""
_TrivialFileCatalog

Object to represent a Trivial File Catalog
and provide functionality to create, modify and write tfc;s as well 
as match against the its rules

Usage:

given a TFC file, invoke readTFC on it. This will return
a TrivialFileCatalog instance that can be used to match LFNs
to PFNs.

Usage: Given a TFC constact string: trivialcatalog_file:/path?protocol=proto


    filename = tfcFilename(tfcContactString)
    protocol = tfcProtocol(tfcContactString)
    tfcInstance = readTFC(filename)

    lfn = "/store/PreProd/unmerged/somelfn.root"

    pfn = tfcInstance.matchLFN(protocol, lfn)

Create and manipulate a trivial file catalog


"""

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery
import re
import os
import urlparse

_TFCArgSplit = re.compile("\?protocol=")


def tfcProtocol(contactString):
    """
    _tfcProtocol_

    Given a Trivial File Catalog contact string, extract the
    protocol from it.

    """
    args = urlparse.urlsplit(contactString)[3]
    value = args.replace("protocol=", '')
    return value

def tfcFilename(contactString):
    """
    _tfcFilename_

    Extract the filename from a TFC contact string.

    """
    value = contactString.replace("trivialcatalog_file:", "")
    value = _TFCArgSplit.split(value)[0]
    path = os.path.normpath(value)
    return path




class TrivialFileCatalog:
    """
    _FwkJobReport_

    Framework Job Report container and interface object

    """
    def __init__(self, url = None):
        self.preferredProtocol = None # attribute for preferred protocol
        self.lfnToPfn = []
        self.pfnToLfn = []
        if url:
            self.load(url)
        
        
    def addLfnToPfnRule(self, protocol, pathMatch, result, chain = None):
        """
        Add a LFN-To-PFN mapping rule that will match for the given
        protocol and destination
        """
        self.lfnToPfn.append(
            {"protocol" : protocol,
             "path-match" : pathMatch,
             "path-match-regexp" : re.compile(pathMatch),
             "chain" : chain,
             "result" : result}
            )
        return


    def matchLFN(self, protocol, lfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match
        """
        if not protocol:
            protocol = self.preferredProtocol
            
        for mapping in self.lfnToPfn:
            if mapping['protocol'] != protocol:
                continue
            if mapping['path-match-regexp'].match(lfn):
                if mapping['chain'] != None:
                    lfn = self.matchLFN(mapping['chain'], lfn)
                try:
                    splitLFN = mapping['path-match-regexp'].split(lfn, 1)[1]
                except IndexError:
                    continue
                result = mapping['result'].replace("$1", splitLFN)
                return result

        return None

    
    def addPfnToLfnRule(self, protocol, pathMatch, result, chain = None):
        """
        Add a PFN-To-LFN mapping rule that will match for the given
        protocol and destination
        """
        self.pfnToLfn.append(
            {"protocol" : protocol,
             "path-match" : pathMatch,
             "path-match-regexp" : re.compile(pathMatch),
             "chain" : chain,
             "result" : result}
            )
        return

        
    def matchPFN(self, protocol, pfn):
        """
        _matchPFN_

        Return the result for the PFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match
        """
        if not protocol:
            protocol = self.preferredProtocol
        
        for mapping in self.pfnToLfn:
            if mapping['protocol'] != protocol:
                continue
            if mapping['path-match-regexp'].match(pfn):
                if mapping['chain'] != None:
                    pfn = self.matchPFN(mapping['chain'], pfn)
                try:
                    splitPFN = mapping['path-match-regexp'].split(pfn, 1)[1]
                except IndexError:
                    continue
                result = mapping['result'].replace("$1", splitPFN)
                return result

        return None


    def save(self):
        """
        _save_

        Save the tfc by converting it into
        an XML IMProv Object

        """
        result = IMProvNode("storage-mapping")
        
        for maping in self.lfnToPfn:
            node = IMProvNode("lfn-to-pfn", None)
            node.attrs.update(maping)
            del node.attrs['path-match-regexp']
            if not maping.get('chain', None):
                del node.attrs['chain']
            result.addNode(node)

        for maping in self.pfnToLfn:
            node = IMProvNode("pfn-to-lfn", None)
            node.attrs.update(maping)
            del node.attrs['path-match-regexp']
            if not maping.get('chain', None):
                del node.attrs['chain']
            result.addNode(node)
        
        return result


    def write(self, filename):
        """
        _write_

        Write the tfc to an XML file

        """
        handle = open(filename, 'w')
        handle.write(self.save().makeDOMElement().toprettyxml())
        handle.close()
        return
    

    def __str__(self):
        """string representation of instance"""
        return str(self.save())
        
        
    def load(self, url):
        """
        _load_

        Read a tfc into this instance

        """
        self.lfnToPfn = []
        self.pfnToLfn = []
        self.preferredProtocol = tfcProtocol(url)
        filename = tfcFilename(url)
        
        if not os.path.exists(filename):
            msg = "TrivialFileCatalog not found: %s" % filename
            raise RuntimeError, msg

        try:
            node = loadIMProvFile(filename)
        except StandardError, ex:
            msg = "Error reading TrivialFileCatalog: %s\n" % filename
            msg += str(ex)
            raise RuntimeError, msg

        query = IMProvQuery("storage-mapping/lfn-to-pfn")
        mappings = query(node)

        for mapping in mappings:
            protocol = mapping.attrs.get("protocol", None)
            match = mapping.attrs.get("path-match", None)
            result = mapping.attrs.get("result", None)
            chain = mapping.attrs.get("chain", None)
            if True in (protocol, match, mapping == None):
                continue
            self.addLfnToPfnRule(str(protocol), str(match), \
                                                        str(result), chain)
            
        
        query = IMProvQuery("storage-mapping/pfn-to-lfn")
        mappings = query(node)

        for mapping in mappings:
            protocol = mapping.attrs.get("protocol", None)
            match = mapping.attrs.get("path-match", None)
            result = mapping.attrs.get("result", None)
            chain = mapping.attrs.get("chain", None)
            if True in (protocol, match, mapping == None):
                continue
            self.addPfnToLfnRule(str(protocol), str(match), \
                                                        str(result), chain)
        
        return
        
    


if __name__ == '__main__':
    tfc= TrivialFileCatalog()
    tfc.addLfnToPfnRule('local', '.*', '/data/')
    print tfc
    print "Now read siteconf tfc"
    tfc.load(os.path.expandvars("$CMS_PATH/SITECONF/local/PhEDEx/storage.xml"))
    print 'look for /store/test/storage.xml'
    pfn = tfc.matchLFN('srm', '/store/test/storage.xml')
    print "found at %s" % pfn
    lfn = tfc.matchPFN('srm', pfn)
    print "which reverse lookups to %s" % lfn
