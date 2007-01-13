#!/usr/bin/env python
"""
_DLSDBS_ wrappers

"""

import logging
import warnings
warnings.filterwarnings("ignore","Python C API version mismatch for module _lfc",RuntimeWarning)

# DBS-1 objects
import dlsClient
from dlsDataObjects import *
import dbsCgiApi
from dbsException import DbsException
# DBS-2
#import dbsApi

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Core.Codes import exceptions


class DBS:

   def __init__(self,dbs_url,dbs_address):
       self.conf={}
       self.conf['url']=dbs_url
       self.conf['address']=dbs_address
       try:
           self.api= dbsCgiApi.DbsCgiApi(dbs_url,\
           {'instance': dbs_address})
       except StandardError, ex:
           raise ProdException(exceptions[4009]+str(ex),4009)

   def getName(self):
        """
        _getName

        return the DBS Instance Name

        """
        dbsName = "%s?instance=%s" % ( self.conf['url'],
                                       self.conf['address'])
        
        return dbsName

   def listFileBlocksForDataset(self, dataset):
        """
        _listFileBlocksForDataset_

        return a list of file block names for a dataset

        """
        fileBlocks = self.api.getDatasetFileBlocks(dataset)
        fileblocks=[]
        for fileblock in fileBlocks:
            fileblocks.append(fileblock['blockName'])
        return fileblocks

   def getDatasetFiles(self, dataset):
        """
        _getDatasetFiles_

        """
        result = {}
        contents = self.api.getDatasetContents(dataset)
        files = []
        for block in contents:
            files.extend(map(extractFile,  block['eventCollectionList']))
        for item in files:
            result[item[0]] = item[1]
        return result

   def getFileBlockFiles(self, dataset):
        """
        _getFileBlockFiles_

        """
        contents = self.api.getDatasetContents(dataset)
        fileBlocks={}
        files = []
        for block in contents:
            files = []
            files.extend(map(extractFile,  block['eventCollectionList']))
            fileBlocks[block['blockName']]=files
        return fileBlocks

class DLS:

   def __init__(self,dls_type,dls_address):
       self.conf={}
       self.conf['type']=dls_type
       self.conf['address']=dls_address
       try:
           logging.debug('Instantiating DLS client interface')
           self.api= dlsClient.getDlsApi(dls_type = dls_type,\
                                     dls_endpoint = dls_address)
           logging.debug('DLS client interface instantiated')
       except dlsApi.DlsApiError, inst:
           raise ProdException(exceptions[4010]+str(inst),4010)

   def getName(self):
        """
        _getName_

        return the DLS Instance Url

        """
        typeMap = {'DLS_TYPE_LFC' : "lfc", 'DLS_TYPE_MYSQL': "mysql"}
        dlsName = "%s://%s" % (
            typeMap[self.conf['type']],
            self.conf['address'])
        return dlsName 

   def getFileBlockLocation(self, fileBlockName):
        """
        _getFileBlockLocation_

        Get a list of fileblock locations

        """
        locations = self.api.getLocations(fileBlockName)
        result = {} 
        for loc in locations:
            result[str(loc.fileBlock.name)]=[]
            for locInst in  loc.locations:
                result[str(loc.fileBlock.name)].append(str(locInst.host))
        return result


def extractFile(evColl):
    """
    _extractFile_

    Convert evColl into LFN + number of events

    """
    name = evColl['collectionName']
    count = evColl['numberOfEvents']
    name = evColl['fileList'][-1]['logicalFileName']
    return (name, count)





