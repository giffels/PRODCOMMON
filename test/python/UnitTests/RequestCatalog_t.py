#!/usr/bin/env python
"""
Unittest for ProdCommon.RequestSpecInterface.RequestSpec classes

"""

import unittest
import logging
from TestHandler import *

from ProdCommon.MCPayloads.RequestSpec import *
from ProdCommon.CatalogInterface.Catalog import *

dbs_url='http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery'
dbs_address='DevMC/Writer'
dls_type='DLS_TYPE_MYSQL'
dls_address='lxgate10.cern.ch:18081'

def getDatasets(request):
   datasetsInfo=request.workflow.inputDatasets()
   datasets=[]
   for datasetInfo in datasetsInfo:
       dataset='/'+datasetInfo['PrimaryDataset']+\
               '/'+datasetInfo['DataTier']+\
               '/'+datasetInfo['ProcessedDataset']
       datasets.append(dataset)
   return datasets

def getFileLocationTuples(request):
   mydbs=DBS(dbs_url,dbs_address)
   mydls=DLS(dls_type,dls_address)
   datasets=getDatasets(request)
   fileLocationTuples=[]
   for dataset in datasets:
       fileblocks=mydbs.listFileBlocksForDataset(dataset)
       fileblockLocations=mydls.getFileBlockLocation(fileblocks)
       files=mydbs.getFileBlockFiles(dataset)
       for fileblock in fileblocks:
           fileLocationTuple={}
           fileLocationTuple['files']=files[fileblock]
           fileLocationTuple['locations']=fileblockLocations[fileblock] 
           fileLocationTuples.append(fileLocationTuple)
   return fileLocationTuples

class RequestCatalogTest(unittest.TestCase):
   
    def setUp(self):
        """setup for tests"""
        logging.getLogger().setLevel(logging.DEBUG)

    def tearDown(self):
        """cleanup after test"""
    
    
    def testA(self):
        try:
           requestSpec=RequestSpec()
           requests=readSpecFile("prodrequestSample.xml")
           print('found: '+str(len(requests))+' requests')
           for request in requests:
               request_type=str(request.requestDetails["type"])
               if request_type=="file":
                   fileLocationTuples=getFileLocationTuples(request)
                   print(">>>FileLocationTuples: "+str(fileLocationTuples))
 
        except StandardError, ex:
            msg = "Failed :\n"
            msg += str(ex)
            self.fail(msg)

if __name__ == '__main__':
    unittest.main()

    
