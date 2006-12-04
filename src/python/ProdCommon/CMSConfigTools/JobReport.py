#!/usr/bin/env python
"""
_JobRpeort_

Tools for manipulating the JobReport settings in the config file

All jobs expect a job report called FrameworkJobReport.xml

"""

from ProdCommon.CMSConfigTools.Utilities import unQuote

def hasService(cfgInterface):
    """
    _hasService_

    Check to see wether the MessageLogger service exists in the cfg

    """
    return "MessageLogger" in cfgInterface.cmsConfig.serviceNames()

    
def hasReport(cfgInterface):
    """
    _hasReport_

    check to see if FrameworkJobReport.xml exists in cfg

    """
    if not hasService(cfgInterface):
        return False
    loggerSvc = cfgInterface.cmsConfig.service("MessageLogger")

    if not loggerSvc.has_key("fwkJobReports"):
        return False
    
    reports = loggerSvc['fwkJobReports']
    reportNames = []
    for item in reports[2]:
        reportNames.append(unQuote(item))

    return "FrameworkJobReport.xml" in reportNames

    

def insertReport(cfgInterface):
    """
    _insertReport_

    Insert the FrameworkJobReport.xml into the config object

    """
    if not hasService(cfgInterface):
        cfgInterface.cmsConfig.psdata['services']['MessageLogger'] = {
            '@classname': ('string', 'tracked', 'MessageLogger'),
            }
    loggerSvc = cfgInterface.cmsConfig.service("MessageLogger")
    if not loggerSvc.has_key("fwkJobReports"):
        loggerSvc['fwkJobReports'] = ("vstring", "untracked", [])

    loggerSvc['fwkJobReports'][2].append("\"FrameworkJobReport.xml\"")
    return

def insertEventLogger(cfgInterface):
    """
    _insertEventLogger_

    Add a message logger destination that writes an EventLog file containing
    the FwkReport messages for live event count monitoring

    Assumes MessageLogger service is already present

    untracked PSet EventLogger = {
    untracked PSet default = { untracked int32 limit = 0 }
    untracked PSet FwkReport  = {
         untracked int32 limit = 10000000
         untracked int32 reportEvery = 1
      }
    }
    """
    loggerSvc = cfgInterface.cmsConfig.service("MessageLogger")


    if not loggerSvc.has_key("destinations"):
        loggerSvc['destinations'] = ('vstring', 'untracked', [])
    destinations = loggerSvc['destinations']
    if "\"EventLogger\"" not in destinations:
        loggerSvc['destinations'][2].append("\"EventLogger\"")

        loggerSvc['EventLogger'] = ('PSet', "untracked", {
            'default' : ( 'PSet', 'untracked', {
            'limit' : ( "int32", "untracked", '0')
            }),
            'FwkReport' : ( 'PSet', 'untracked', {
            'limit' : ( "int32", "untracked", '1000000'),
            'reportEvery' :( "int32", "untracked", '1'),
            
            }),
            
            })
        
    return

        

def checkJobReport(cfgInterface):
    """
    _checkJobReport_

    Make sure the job report entry is present, if not insert it

    """
    if hasReport(cfgInterface):
        return
    insertReport(cfgInterface)
    insertEventLogger(cfgInterface)
    return



