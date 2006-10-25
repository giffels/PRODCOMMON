#!/usr/bin/env python
"""
_JobRpeort_

Tools for manipulating the JobReport settings in the config file

All jobs expect a job report called FrameworkJobReport.xml

"""

from CMSConfigTools.Utilities import unQuote

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


def checkJobReport(cfgInterface):
    """
    _checkJobReport_

    Make sure the job report entry is present, if not insert it

    """
    if hasReport(cfgInterface):
        return
    insertReport(cfgInterface)
    return



