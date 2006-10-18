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


#NOTE: we use the convention that we pass the configuration
#NOTE: object which has a "DB" block

def loadConf(configuration=None):
    try:
       if configuration!=None:
          configObject=configuration()
          dbConfig=configObject.getConfig("DB")
          defaultConfig.update(dbConfig)
    except StandardError, ex:
        msg = "ProdDB.Config:"
        msg += "Unable to load Config for ProdDB\n"
        msg += "%s\n" % ex
        logging.warning(msg)


# NOTE: these files need to be overwritten by values from the config file.
defaultConfig={'dbName':'ChangeMe',
               'host':'ChangeMe',
               'user':'ChangeMe',
               'passwd':'ChangeMe',
               'socketFileLocation':'ChangeMe',
               'portNr':'',
               'refreshPeriod' : 4*3600 ,
               'maxConnectionAttempts' : 5,
               'dbWaitingTime' : 10 
              }


        
