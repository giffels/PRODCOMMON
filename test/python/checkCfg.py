#!/usr/bin/env python
"""
_checkCfg_

Create a workflow using a configuration PSet.


"""
__version__ = "$Revision: $"
__revision__ = "$Id: $"

import os
from os import getenv
import sys
import getopt
import popen2
import time
import string

import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
from ProdCommon.MCPayloads.WorkflowMaker import WorkflowMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


def checkcreateWorkflow(cfgFile):
    """
    """
    try:
        #  //
        # // Set CMSSW_SEARCH_PATH
        #//
        pwd = getenv ("PWD")
        oldCmsswSearchPath = getenv ("CMSSW_SEARCH_PATH")
        if not oldCmsswSearchPath:
           msg = "CMSSW_SEARCH_PATH not set....you need to set CMSSW environment "
           raise RuntimeError, msg
        #os.environ["CMSSW_SEARCH_PATH"] = "%s:%s" % (pwd, oldCmsswSearchPath)
        os.environ["CMSSW_SEARCH_PATH"] = "/:%s" % (oldCmsswSearchPath)
        #  //
        # // convert cfg
        #//
        # for the time being we support only cfg file. We might have to support multiple types soon
        print ">>> Checking cfg %s"%cfgFile
        if cfgType == "cfg":
            from FWCore.ParameterSet.Config import include
            cmsCfg = include(cfgFile)
        else:
            modRef = imp.find_module( os.path.basename(cfgFile).replace(".py", ""),  os.path.dirname(cfgFile))
            cmsCfg = modRef.process
                                                                                                                
        cfgWrapper = CMSSWConfig()
        cfgWrapper.originalCfg = file(cfgFile).read()
        cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
        cfgInt.validateForProduction()
    except Exception, e:
        print "Unable to create request: %s" % e


if __name__ == "__main__":


 valid = ['cfg=','py-cfg=','cfgFileList=']

 usage = "Usage: checkCfg.py --cfg=<cfgFile>\n"
 usage += "                  --cfgFileList==<cfgFileList>\n"
 usage += "\n"
 usage += "You must have a scram runtime environment setup to use this tool\n"
 usage += "since it will invoke EdmHash tools\n\n"
 usage += "You must have the PRODCOMMON env set \n"

 try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
 except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

 cfgFile = None
 cfgFileList = None
 cfgType = "cfg"


 for opt, arg in opts:
    if opt == "--cfg":
        cfgFile = arg
        cfgType = "cfg"
    if opt == "--py-cfg":
        cfgFile = arg
        cfgType = "python"
    if opt == "--cfgFileList":
        cfgFileList = arg
        cfgType = "cfg"


 if (cfgFile == None) and (cfgFileList == None) :
    msg = "\n either --cfg or --cfgFileList option has to be provided"
    raise RuntimeError, msg
 if (cfgFile != None) and (cfgFileList != None) :
    msg="\n options --cfg or --cfgFileList are mutually exclusive"
    raise RuntimeError, msg
 
 if (cfgFileList != None) :
    expand_cfgFileList=os.path.expandvars(os.path.expanduser(cfgFileList))
    if not os.path.exists(expand_cfgFileList):
      msg= "File not found: %s" % expand_cfgFileList
      raise RuntimeError, msg
                                                                                                                                       
    cfglist_file = open(expand_cfgFileList,'r')
    for line in cfglist_file.readlines():
      expand_cfg=os.path.expandvars(os.path.expanduser(string.strip(line)))
      checkcreateWorkflow(expand_cfg)
    cfglist_file.close()

 if (cfgFile != None) :
    if not os.path.exists(cfgFile):
      msg = "Cfg File Not Found: %s" % cfgFile
      raise RuntimeError, msg

    checkcreateWorkflow(cfgFile)


