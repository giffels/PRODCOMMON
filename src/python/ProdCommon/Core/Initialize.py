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
   logFile=compConf.get('Core')['logFile']
   processLogFile=logFile+"_process"+'_'+str(os.getpid())+'.log'
   logHandler = RotatingFileHandler(processLogFile, "a", 1000000, 3)
   logFormatter = logging.Formatter("%(asctime)s:%(message)s")
   logHandler.setFormatter(logFormatter)
   logging.getLogger().addHandler(logHandler)

   config.update(compConf.get('Core'))
   db_config.update(compConf.get('DB'))
   if config['debug']=='on':
       logging.getLogger().setLevel(logging.DEBUG)

initialize()

