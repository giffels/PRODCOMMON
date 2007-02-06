#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from ProdCommon.DataMgmt.DBS.DBSErrors import DBSReaderError, formatEx



class DBSReader:
    """
    _DBSReader_

    General API for reading data from DBS


    """
    def __init__(self, url,  **contact):
        args = { "url" : url}
        args.update(contact)
        self.dbs = DbsApi(args)


    def listPrimaryDatasets(self, match = None):
        """
        _listPrimaryDatasets_
        
        return a list of primary datasets matching the glob expression.
        If no expression is provided, all datasets are returned
        """
        arg = "*"
        if match != None:
            arg = match
        try:
            result = self.dbs.listPrimaryDatasets(arg)
        except DbsException, ex:
            msg = "Error in DBSReader.listPrimaryDataset(%s)\n" % arg
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
            
        result = [ x['Name'] for x in result ]
        return result


    def listProcessedDatasets(self, primary, dataTier = None):
        """
        _listProcessedDatasets_

        return a list of Processed datasets for the primary and optional
        data tier value

        """
        tier = "*"
        if dataTier != None:
            tier = dataTier

        try:
            result = self.dbs.listProcessedDatasets(primary, tier)
        except DbsException, ex:
            msg = "Error in DBSReader.listProcessedDatasets(%s)\n" % primary
            msg += "%s\n" % formatEx(ex)
            raise DBSReaderError(msg)
        
        result = [ x['Name'] for x in result ]
        return result
        
    
