import logging
from logging.handlers import RotatingFileHandler
import os
import time

from ProdCommon.Core.Configuration import loadProdCommonConfiguration
from ProdCommon.Database.Config import loadConf

db_config={}
configuration=None

def initialize():
   global config 
   global configuration
 
   compConf=loadProdCommonConfiguration()
   db_config.update(compConf.get('DB'))
   configuration=compConf

initialize()

