#!/usr/bin/env python
"""
_DatasetExpander_

Expand a Multi Tier output module such as GEN-SIM-DIGI-RECO
into four basic tier datasets


"""

from ProdCommon.MCPayloads.DatasetInfo import DatasetInfo

def isMultiTier(dataTier):
    """
    _isMultiTier_

    return True if the dataTier provided is a multi tier

    """
    return dataTier.find("-") > 0


def splitMultiTier(multiTier):
    """
    _splitMultiTier_

    Break a multi tier into a list of basic tiers, if multi tier is
    already a basic tier, then it gets returned as a single element list
    as is.
    """
    result = []
    if isMultiTier(multiTier):
        for tier in multiTier.split("-"):
            if len(tier.strip()) == 0:
                continue
            result.append(tier)
    else:
        result.append(multiTier)
    return result


def expandDatasetInfo(datasetInfo, requestTimestamp):
    """
    _expandDatasetInfo_

    Given a DatasetInfo, check to see if it contains multi tiers,
    and if it does, expand them to a list of basic tier dataset info objects

    returns a list of datasetInfo objects

    """
    result = []
    if not isMultiTier(datasetInfo['DataTier']):
        #  //
        # // No multi tier, return same object as only entry in list
        #//
        result.append(datasetInfo)
        return result
    
    tiers = splitMultiTier(datasetInfo['DataTier'])
    processedDSName = "%s-%s-%s" % (datasetInfo['ApplicationVersion'],
                                    datasetInfo['OutputModuleName'],
                                    requestTimestamp)
    for tier in tiers:
        newInfo = DatasetInfo()
        newInfo.update(datasetInfo)
        newInfo['DataTier'] = tier
        newInfo['ProcessedDataset'] = processedDSName
        result.append(newInfo)
    return result



