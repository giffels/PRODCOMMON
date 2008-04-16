#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

SITEDB_JSON="https://cmsweb.cern.ch/sitedb/sitedb/json/index/"


import urllib

def getJSON(service, **args):
    """
    _getJSON_

    retrieve JSON formatted information given the service name and the
    argument dictionaries

    """
    query = SITEDB_JSON
    query += "%s" % service

    
    
    params = urllib.urlencode(args)
    f = urllib.urlopen(query, params)
    result = f.read()
    f.close()
    output = eval(result)
    return output
    

if __name__ == '__main__':

    print getJSON("dnUserName", dn="/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson")
    
    print getJSON("CEtoCMSName", name="a01-004-128.gridka.de")
    
