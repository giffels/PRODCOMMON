#!/usr/bin/env python
"""
_DatasetConventions_

Methods that generate dataset names based on established conventions,
attempting to provide a single point for them to be changed
without destroying interfaces

"""

from ProdCommon.Core.ProdException import ProdException

class DatasetConvError(ProdException):
    """
    _DatasetConvError_

    All Exceptions from this module are this kind
    
    """
    def __init__(self, message, **data):
        ProdException.__init__(self, message, 11000, **data)


def primaryDatasetName(**args):
    """
    _primaryDatasetName_

    Create a primary dataset name from the arguments provided

    Throws DatasetConvError if arguments are missing or invalid

    """
    if args.get("PhysicsChannel", None) == None:
        msg = "PhysicsChannel Not Specified\n"
        msg += "This is required for a primary dataset name"
        raise DatasetConvError(msg)

    channel = args['PhysicsChannel']
    #channel = channel.upper()
    #channel = channel.lower()
    return channel

    


def processedDatasetName(**args):
    """
    _processedDatasetName_

    Create a Processed Dataset name

    Throws DatasetConvError if arguments are missing or invalid
    
    """
    checkArgs = ['Version', 'Label', 'RequestId']

    for arg in checkArgs:
        if args.get(arg, None) == None:
            msg = "%s Not Specified\n" % arg
            msg += "This is required for a processed dataset name"
            raise DatasetConvError(msg)

    datasetName = "%s-%s-%s" % (args['Version'],
                             args['Label'],
                             args['RequestId'])
    
    filterName = args.get("FilterName", None)
    if filterName != None:
        datasetName += "-%s" % filterName

    isUnmerged = args.get("Unmerged", False)
    if isUnmerged:
        datasetName += "-unmerged"
    return datasetName


def tier0ProcessedDatasetName(**args):
    """
    _tier0ProcessedDatasetName_

    Create a Tier 0 Convention Processed Dataset name
 
    Throws DatasetConvError if arguments are missing or invalid
    
    """
    checkArgs = ['Version', 
                 'InputProcessedDataset']
    
    for arg in checkArgs:
        if args.get(arg, None) == None:
            msg = "%s Not Specified\n" % arg
            msg += "This is required for a processed dataset name"
            raise DatasetConvError(msg)

    datasetName = "%s-%s" % (args['InputProcessedDataset'],
                                args['Version'])
    

    isUnmerged = args.get("Unmerged", False)
    if isUnmerged:
        datasetName += "-unmerged"
    return datasetName



def csa08ProcessedDatasetName (**args):
    """
    _csa08ProcessedDatasetName_

    Create a dataset name based on csa08 conventions

    """
    checkArgs = ['AcquisitionEra','Conditions',
                 'ProcessingVersion']

    for arg in checkArgs:
        if args.get(arg, None) == None:
            msg = "%s Not Specified\n" % arg
            msg += "This is required for a processed dataset name"
            raise DatasetConvError(msg)

    if args['FilterName'] in ("",None):
       datasetName = "%s_%s_%s" % (args['AcquisitionEra'],args['Conditions'],
                                   args['ProcessingVersion'])
    else:
       datasetName = "%s_%s_%s_%s" % (args['AcquisitionEra'],args['Conditions'],
                                   args['FilterName'],args['ProcessingVersion'])


    isUnmerged = args.get("Unmerged", False)
    if isUnmerged:
        datasetName += "-unmerged"
    return datasetName


def properProcessedDatasetName (**args):
    """
    _properProcessedDatasetName_

    Create a dataset name based on conventions

    """
    checkArgs = ['AcquisitionEra',
                 'ProcessingVersion']

    for arg in checkArgs:
        if args.get(arg, None) == None:
            msg = "%s Not Specified\n" % arg
            msg += "This is required for a processed dataset name"
            raise DatasetConvError(msg)

    if args.get('ProcessingString', None) == None:
       datasetName = "%s-%s" % (args['AcquisitionEra'],
                                args['ProcessingVersion'])
    else:
       datasetName = "%s-%s-%s" % (args['AcquisitionEra'],
                                   args['ProcessingString'],
                                   args['ProcessingVersion'])

    isUnmerged = args.get("Unmerged", False)
    if isUnmerged:
        datasetName += "-unmerged"

    return datasetName


def checkDataTier(dataTier):
    """
    _checkDataTier_

    Make sure the data tier provided conforms to whatever conventions
    are needed

    """
    return dataTier.upper()

def parseDatasetPath(datasetPath):
    """
    _parseDatasetPath_

    chops up the dataset path provided and returns a dictionary
    of Primary, Processed, DataTier

    """
    result = {}
    result.setdefault("Primary", None)
    result.setdefault("Processed", None)
    result.setdefault("DataTier", None)
    result.setdefault("Analysis", None)
    
    
    while datasetPath.startswith("/"):
        datasetPath = datasetPath[1:]
    datasetSplit = datasetPath.split("/")
    elems = len(datasetSplit)
    if elems == 1:
        result['Primary'] = datasetSplit[0]
        return result
    elif elems == 2:
        result['Primary'] = datasetSplit[0]
        result['Processed'] = datasetSplit[1]
        return result
    elif elems == 3:
        result['Primary'] = datasetSplit[0]
        result['Processed'] = datasetSplit[1]
        result['DataTier'] = datasetSplit[2]
        return result
    
    result['Primary'] = datasetSplit[0]
    result['Processed'] = datasetSplit[1]
    result['DataTier'] = datasetSplit[2]
    result['Analysis'] = datasetSplit[3]
    return result
