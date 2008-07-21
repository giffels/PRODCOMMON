#!/usr/bin/env python
"""
_Utilities_

Methods of general use for operating on a configuration object

"""

from ProdCommon.CMSConfigTools.ConfigAPI.CMSConfigError import CMSConfigError

import FWCore.ParameterSet.Types  as CfgTypes
import FWCore.ParameterSet.Modules  as CfgModules


class _CfgNoneType:
    """
    None with a value method to avoid having to add extra loops
    to test if a value is None or not
    """
    def value(self):
        return None
    

def hasParameter(pset, *params):
    """
    _hasParameter_

    check that pset provided has the attribute chain
    specified.

    Eg, if params is [ 'attr1', 'attr2', 'attr3' ]
    check for pset.attr1.attr2.attr3

    returns True if parameter exists, False if not

    """
    lastParam = pset
    for param in params:
        lastParam = getattr(lastParam, param, None)
        if lastParam == None:
            return False
    if lastParam != None:
        return True
    return False

def getParameter(pset, *params):
    """
    _getParameter_

    Retrieve the specified parameter from the PSet Provided
    given the attribute chain

    returns None if not found

    """
    lastParam = pset
    for param in params:
        lastParam = getattr(lastParam, param, None)
        if lastParam == None:
            return None
    return lastParam


def checkMessageLoggerSvc(cfgInstance):
    """
    _checkMessageLogger_

    Check that the message logger is active and provides the default
    job report and event logger settings for production
    
    """

    svcs = cfgInstance.services

    #  //
    # // Check Job report service will write the expected 
    #//  FrameworkJobReport.xml file
    if not svcs.has_key('MessageLogger'):
        cfgInstance.add_(CfgModules.Service("MessageLogger"))
        
    messageLogger = cfgInstance.services['MessageLogger']    
    
    
    if "fwkJobReports" not in messageLogger.parameterNames_():
        messageLogger.fwkJobReports = CfgTypes.untracked(
            CfgTypes.vstring()
            )        
        
    if "FrameworkJobReport" not in messageLogger.fwkJobReports:
        messageLogger.fwkJobReports.append("FrameworkJobReport")

    if "FrameworkJobReport" not in messageLogger.parameterNames_():
        messageLogger.FrameworkJobReport = CfgTypes.untracked(CfgTypes.PSet())
        

    messageLogger.FrameworkJobReport.default = CfgTypes.untracked(
        CfgTypes.PSet(
        limit = CfgTypes.untracked(CfgTypes.int32(0))
        )
        )
    messageLogger.FrameworkJobReport.FwkJob = CfgTypes.untracked(
        CfgTypes.PSet(
        limit = CfgTypes.untracked(CfgTypes.int32(10000000))
        ) )

    #  //
    # // Install the per event output
    #//
    if "destinations" not in messageLogger.parameterNames_():
        messageLogger.destinations = CfgTypes.untracked(
            CfgTypes.vstring()
            )        

    if "EventLogger" not in messageLogger.destinations:
        messageLogger.destinations.append("EventLogger")

    
    if "EventLogger" not in messageLogger.parameterNames_():
        messageLogger.EventLogger = CfgTypes.untracked(CfgTypes.PSet())

    messageLogger.EventLogger.default = CfgTypes.untracked(
        CfgTypes.PSet(
        limit = CfgTypes.untracked(CfgTypes.int32(0))
        )
        )

    messageLogger.FrameworkJobReport.FwkReport = CfgTypes.untracked(
        CfgTypes.PSet(
        limit = CfgTypes.untracked(CfgTypes.int32(10000000)),
        reportEvery = CfgTypes.untracked(CfgTypes.int32(10000000))
        ) )

    return

def checkConfigMetadata(cfgInstance):
    """
    _checkConfigMetadata_

    Ensure that the cfgInstance contains a PSet called configurationMetadata
    and that it contains the required fields
    
    """

    if "configurationMetadata" not in  cfgInstance.psets.keys():
        msg = "No configurationMetadata PSet found in config file\n"
        raise CMSConfigError( msg)
    
    cfgMeta = cfgInstance.psets['configurationMetadata']

    if cfgMeta.isTracked():
        msg = "configurationMetadata PSet is tracked.\n"
        msg += "This PSet should be untracked\n"
        raise CMSConfigError( msg)

    requiredAttrs = [
        'name',
        'version',
        'annotation',
        ]
    for reqAttr in requiredAttrs:
        attr = getattr(cfgMeta, reqAttr, None)
        
        if attr == None:
            msg = "Missing configurationMetadata Parameter: %s\n" % reqAttr
            msg += "This parameter must be set for production cfg files\n"
            raise CMSConfigError(msg, MissingParameter = reqAttr)

        if attr.isTracked():
            msg = "Attribute configurationMetadata.%s is Tracked\n" % reqAttr
            msg += "This attribute should be untracked\n"
            raise CMSConfigError(msg, TrackedParameter = reqAttr)
        
    return


def checkOutputModule(outModInstance):
    """
    _checkOutputModule_

    Check that an output module instance has a dataset PSet in it

    """
    
    datasetPSet =  getattr(outModInstance, "dataset", None)
    if datasetPSet == None:
        msg = "No Dataset PSet for Output Module: %s\n" % outModInstance
        msg += "All Output Modules should contain a dataset PSet for"
        msg += " production requests"
        raise CMSConfigError( msg, OutputModule = str(outModInstance))
    dataTier = getattr(datasetPSet, "dataTier", None)
    if dataTier == None:
        msg = "Output Module %s dataset PSet does not contain " % (
            outModInstance,
            )
        msg += "dataTier\n This is required for Production requests\n"
        raise CMSConfigError( msg, OutputModule = str(outModInstance),
                              MissingParameter = "dataTier")
                                                      

    if dataTier.isTracked():
        msg = "%s.dataset.dataTier is Tracked\n" % outModInstance
        msg += "This parameter should be untracked\n"
        raise CMSConfigError( msg, OutputModule = str(outModInstance),
                              TrackedParameter = "dataTier")


    checkVal = dataTier.value().strip()
    if len(checkVal) == 0:
        msg = "%s.dataset.dataTier is Empty\n" % outModInstance
        msg += "This parameter needs to be a valid data Tier string\n"
        raise CMSConfigError( msg, OutputModule = str(outModInstance),
                              BadParameter = "dataTier")
        
    return


def seedCount(cfgInstance):
    """
    _seedCount

    get number of required seeds for the cfg Process instance provided
    
    """
    if "RandomNumberGeneratorService" not in cfgInstance.services.keys():
        return 0
    svc = cfgInstance.services["RandomNumberGeneratorService"]

    sourceSeeds = 1
    #  //
    # // How many source seeds are present?
    #//  If vector sourceSeedsVector present, then it is the length
    #  //of that vector
    # //
    #//
    srcSeedVec = getattr(svc, "sourceSeedVector", _CfgNoneType()).value()
    if srcSeedVec != None:
        sourceSeeds = len(srcSeedVec)
        
    modSeeds = getattr(svc, "moduleSeeds", _CfgNoneType()).value()
    if modSeeds == None:
        seedsReq = sourceSeeds
    else:
        seedsReq = len(modSeeds.parameterNames_()) + sourceSeeds

    #  //
    # // spares never hurt either...
    #// 
    return seedsReq +2
