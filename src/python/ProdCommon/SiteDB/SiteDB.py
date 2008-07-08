#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

__revision__ = "$Id: CondorHandler.py,v 1.2 2008/05/21 19:49:44 ewv Exp $"
__version__ = "$Revision: 1.2 $"

import urllib
import cStringIO
import tokenize

class SiteDBJSON:

  SITEDB_JSON="https://cmsweb.cern.ch/sitedb/sitedb/json/index/"

  def dnUserName(self,dn):
    userinfo = self.getJSON("dnUserName", dn=dn)
    userName = userinfo['user']
    return userName


  def getJSON(self,service, **args):
    """
    _getJSON_

    retrieve JSON formatted information given the service name and the
    argument dictionaries

    """
    query = self.SITEDB_JSON
    query += "%s" % service

    params = urllib.urlencode(args)
    f = urllib.urlopen(query, params)
    result = f.read()
    f.close()

    output = self.dictParser(result)
    return output


  def _parse(self,token, src):
    """
    Dictionary string parser from
    Fredrik Lundh (fredrik at pythonware.com)
    on python-list
    """
    if token[1] == "{":
      out = {}
      token = src.next()
      while token[1] != "}":
        key = self._parse(token, src)
        token = src.next()
        if token[1] != ":":
	  raise SyntaxError("Malformed dictionary")
        value = self._parse(src.next(), src)
        out[key] = value
        token = src.next()
        if token[1] == ",":
	  token = src.next()
      return out
    elif token[1] == "[":
      out = []
      token = src.next()
      while token[1] != "]":
        out.append(_parse(token, src))
        token = src.next()
        if token[1] == ",":
          token = src.next()
      return out
    elif token[0] == tokenize.STRING:
      return token[1][1:-1].decode("string-escape")
    elif token[0] == tokenize.NUMBER:
      try:
        return int(token[1], 0)
      except ValueError:
        return float(token[1])
    else:
      raise SyntaxError("Malformed expression")


  def dictParser(self,source):
     src = cStringIO.StringIO(source).readline
     src = tokenize.generate_tokens(src)
     return self._parse(src.next(), src)



if __name__ == '__main__':

    mySiteDB = SiteDBJSON()

    print mySiteDB.getJSON("dnUserName", dn="/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson")

    print mySiteDB.getJSON("CEtoCMSName", name="cmsosgce.fnal.gov")

