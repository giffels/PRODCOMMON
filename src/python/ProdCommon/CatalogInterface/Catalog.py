#!/usr/bin/env python
"""
_DLSDBS_ wrappers

"""

import logging
import warnings
warnings.filterwarnings("ignore","Python C API version mismatch for module _lfc",RuntimeWarning)
import dlsClient
from dlsDataObjects import *

import dbsCgiApi
from dbsException import DbsException

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
        try:
            fileBlocks = self.api.getDatasetFileBlocks(dataset)
        except DbsException, ex:
            msg = "DbsException for DBS API getDatasetFileBlocks:\n"
            msg += "  Dataset = %s\n" % dataset
            msg += "  Exception Class: %s\n" % ex.getClassName()
            msg += "  Exception Message: %s\n" % ex.getErrorMessage()
            logging.error(msg)
            return []
        fileblocks=[]
        for fileblock in fileBlocks:
            fileblocks.append(fileblock['blockName'])
        return fileblocks

   def getDatasetFiles(self, dataset):
        """
        _getDatasetFiles_

        """
        result = {}
        try:
            contents = self.api.getDatasetContents(dataset)
        except DbsException, ex:
            msg = "DbsException for DBS API getDatasetContents:\n"
            msg += "  Dataset = %s\n" % dataset
            msg += "  Exception Class: %s\n" % ex.getClassName()
            msg += "  Exception Message: %s\n" % ex.getErrorMessage()
            logging.error(msg)
            return {}
        files = []
        for block in contents:
            files.extend(map(extractFile,  block['eventCollectionList']))
        for item in files:
            result[item[0]] = item[1]
        return result


class DLS:

   def __init__(self,dls_type,dls_address):
       self.conf={}
       self.conf['type']=dls_type
       self.conf['address']=dls_address
       try:
           self.api= dlsClient.getDlsApi(dls_type = dls_type,\
                                     dls_endpoint = dls_address)
       except dlsApi.DlsApiError, inst:
           raise ProdException(exceptions[4010]+str(ex),4010)

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
        try:
            locations = self.api.getLocations(fileBlockName)
        except dlsApi.DlsApiError, ex:
            msg = "Error in the DLS query: %s\n" % str(ex)
            msg += "When trying to get locations for file Block:\n"
            msg += "%s\n" % fileBlockName
            logging.error(msg)
            return []
        result = []
        for loc in locations:
            for locInst in  loc.locations:
                host = locInst.host
                if host not in result:
                    result.append(host)
        return result


def extractFile(evColl):
    """
    _extractFile_

    Convert evColl into LFN + number of events

    """
    name = evColl['collectionName']
    count = evColl['numberOfEvents']
    name = evColl['fileList'][-1]['logicalFileName']
    return name, count





