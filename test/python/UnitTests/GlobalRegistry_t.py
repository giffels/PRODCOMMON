
#!/usr/bin/env python
"""
Unittest for ProdCommon.Core.GlobalRegistry class

"""

import unittest

import logging

from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.GlobalRegistry import registerHandler 
from TestHandler import *

class GlobalRegistryTest(unittest.TestCase):
   
    def setUp(self):
        """setup for tests"""
        logging.getLogger().setLevel(logging.DEBUG)

    def tearDown(self):
        """cleanup after test"""
    
    
    def testA(self):
        try:
           registerHandler(TestHandler1(),"testHandler1","registry1")
           try:
               registerHandler(TestHandler2(),"testHandler1","registry1")
           except:
               pass
           registerHandler(TestHandler3(),"testHandler2","registry1")
           registerHandler(TestHandler3(),"testHandler2","registry2")
           registerHandler(TestHandler3(),"testHandler2","registry3")
           for i in xrange(4,10):
               registerHandler(TestHandler1(),"testHandler1","registry"+str(i))
               registerHandler(TestHandler2(),"testHandler2","registry"+str(i))
               registerHandler(TestHandler3(),"testHandler3","registry"+str(i))
            
        except StandardError, ex:
            msg = "Failed :\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
       try:
           for i in xrange(4,10):
               handler=retrieveHandler("testHandler1","registry"+str(i))
               self.assertEqual(handler.handlerMethod(),"TestHandler1")
               handler=retrieveHandler("testHandler2","registry"+str(i))
               self.assertEqual(handler.handlerMethod(),"TestHandler2")
               handler=retrieveHandler("testHandler3","registry"+str(i))
               self.assertEqual(handler.handlerMethod(),"TestHandler3")
       except StandardError, ex:
           msg = "Failed :\n"
           msg += str(ex)
           self.fail(msg)
             

if __name__ == '__main__':
    unittest.main()

    
