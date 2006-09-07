import logging
from logging.handlers import RotatingFileHandler
import os
import time

from ProdCommon.Core.Configuration import loadProdCommonConfiguration
from ProdCommon.Database.Config import loadConf

config={}
db_config={}

def initialize():
   global config 
   global policies
 
   compConf=loadProdCommonConfiguration()
   config.update(compConf.get('Core'))
   db_config.update(compConf.get('DB'))

initialize()

