#!/usr/bin/env python
"""
Unittest for ProdCommon.CatalogInterface.Catalog classes

"""

import unittest
import logging
from TestHandler import *

from ProdCommon.CatalogInterface.Catalog import *

class CatalogTest(unittest.TestCase):
   
    def setUp(self):
        """setup for tests"""
        logging.getLogger().setLevel(logging.DEBUG)
        self.dbs_url1='http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery'
        self.dbs_address1='DevMC/Writer'
        self.dbs_url2='http://cmsdbs.cern.ch/cms/prod/comp/DBS/CGIServer/prodquery'
        self.dbs_address2='MCLocal_1/Writer'
        self.dls_type='DLS_TYPE_MYSQL'
        self.dls_address='lxgate10.cern.ch:18081'

    def tearDown(self):
        """cleanup after test"""
    
    
    def testA(self):
        try:
            mydbs1=DBS(self.dbs_url1,self.dbs_address1)
            mydls=DLS(self.dls_type,self.dls_address)
            print('DBS name: '+mydbs1.getName())
            print('DLS name: '+mydls.getName())
            datasets=['/MC-110-os-minbias/SIM/CMSSW_1_1_0-GEN-SIM-1161611489-unmerged',\
               '/LPCMC-110-QCD_pt_15_20/SIM/CMSSW_1_1_0-GEN-SIM-DIGI-1161635385',\
               '/LPCMC-111-QCD_pt_80_120/SIM/CMSSW_1_1_1-GEN-SIM-DIGI-1162423692']
            for dataset in datasets:
                print('>>>Dataset: '+dataset)
                fileblocks=mydbs1.listFileBlocksForDataset(dataset)
                print('>>>Fileblocks: '+str(fileblocks))
                fileblockLocations=mydls.getFileBlockLocation(fileblocks)
                print('>>>FileblockLocations: '+str(fileblockLocations))
                files=mydbs1.getDatasetFiles(dataset)
                print('>>>Files: '+str(files))
                print('*****************************')
                print('*****************************')
            mydbs2=DBS(self.dbs_url2,self.dbs_address2)
            print('DBS name: '+mydbs2.getName())
            datasets=['/CSA06-106-os-EWKSoup0-0/RECO/CMSSW_1_1_1-RECO-1164228397-unmerged']
            for dataset in datasets:
                print('>>>Dataset: '+dataset)
                fileblocks=mydbs2.listFileBlocksForDataset(dataset)
                print('>>>Fileblocks: '+str(fileblocks))
                fileblockLocations=mydls.getFileBlockLocation(fileblocks)
                print('>>>FileblockLocations: '+str(fileblockLocations))
                files=mydbs2.getDatasetFiles(dataset)
                print('>>>Files: '+str(files))
                print('*****************************')
                print('*****************************')
        except StandardError, ex:
            msg = "Failed :\n"
            msg += str(ex)
            self.fail(msg)

if __name__ == '__main__':
    unittest.main()

    
