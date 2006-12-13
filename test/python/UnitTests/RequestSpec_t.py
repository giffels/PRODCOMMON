#!/usr/bin/env python
"""
Unittest for ProdCommon.RequestSpecInterface.RequestSpec classes

"""

import unittest
import logging
from TestHandler import *

from ProdCommon.MCPayloads.RequestSpec import *

def parseNewRequest(request):
   print('workflow: '+str(request.workflow.makeIMProv().makeDOMElement().toprettyxml()))
   print('request type: '+str(request.requestDetails["type"]))
   print('owner: '+str(request.requestDetails["owner"]))
   print('id: '+str(request.requestDetails["id"]))



class RequestSpecTest(unittest.TestCase):
   
    def setUp(self):
        """setup for tests"""

    def tearDown(self):
        """cleanup after test"""
    
    
    def testA(self):
        try:
           requestSpec=RequestSpec()
           requests=readSpecFile("prodrequestSample.xml")
           print('found: '+str(len(requests))+' requests')
           for request in requests:
               parseNewRequest(request)
        except StandardError, ex:
            msg = "Failed :\n"
            msg += str(ex)
            self.fail(msg)

if __name__ == '__main__':
    unittest.main()

    
