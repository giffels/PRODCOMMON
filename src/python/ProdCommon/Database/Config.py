#!/usr/bin/env python
"""
_Config_

Get Configuration for ProdDb from the Production config file.
This should be available through PROD_CONFIG env var

If the configuration is not available, the default config settings
here will be used.

"""
import os
import logging

from ProdCommon.Core.ProdException import ProdException

#NOTE: Need to make a distinction for prodagent and prodmanager
#NOTE: when loading the configuration.

def loadConf():
    try:
       #NOTE: do nothing for the moment.
       i='do_nothing'
    except StandardError, ex:
        msg = "ProdDb.Config:"
        msg += "Unable to load ProdAgent Config for ProdDb\n"
        msg += "%s\n" % ex
        logging.warning(msg)


defaultConfig={'dbName':'ProdDb',
               'host':'localhost',
               'user':'Proddie',
               'passwd':'ProddiePass',
               'socketFileLocation':'/opt/openpkg/var/mysql/mysql.sock',
               'portNr':'',
               'refreshPeriod' : 4*3600 ,
               'maxConnectionAttempts' : 5,
               'dbWaitingTime' : 10 
              }

try:
   loadConf()
except Exception,ex:
   raise ProdException(str(ex))

        
