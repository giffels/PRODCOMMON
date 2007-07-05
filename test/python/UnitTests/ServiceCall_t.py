#!/usr/bin/env python
"""
Unittest for ProdCommon.QueryObject.Request class


"""
import time
import logging
import unittest

from ProdCommon.WebServices import serviceCall
from ProdCommon.Database import Session
from ProdCommon.Core.Initialize import configuration


class ServiceCallTest(unittest.TestCase):
   """
   TestCase for robust service calls 
   """
   
   def setUp(self):
       """setup for tests"""
       self.prodagents=20
       self.components=10
       logging.getLogger().setLevel(logging.DEBUG)

   def testA(self):
       job_parameters={'numberOfJobs':20,
                   'prefix':'Wave1'}
       job_result=[{'jobSpecId':'jobspec1',\
                    'URL':'http://somewhere',\
                    'start_event':300,\
                    'end_event':350},
                   {'jobSpecId':'jobspec2',\
                    'URL':'http://somewhere',\
                    'start_event':351,\
                    'end_event':400},
                   {'jobSpecId':'jobspec3',\
                    'URL':'http://somewhere',\
                    'start_event':400,\
                    'end_event':450}]
       allocation_result=["myRequest/1","myRequest/2","myRequest/3","myRequest4"]

       try:
           print("testA")
           Session.set_database(configuration.get('DB'))
           for i in xrange(0,self.prodagents):
               for j in xrange(0,self.components):
                   serviceCall.log("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"acquireAllocation",\
                       ["prodAgentID","myRequest",15],\
                       allocation_result,"my_tag1")
                   logged_result=serviceCall.retrieve("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"my_tag1","acquireAllocation")
                   self.assertEqual(logged_result[0],allocation_result)

                   try:
                       logged_result=serviceCall.retrieve("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"my_wrong_tag1","acquireAllocation")
                   except Exception,ex:
                       print(str(ex))
                       print('INTENTIONAL ERROR!')
                   serviceCall.log("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"acquireJobs",\
                       ["prodAgentID","myRequest",job_parameters],\
                       job_result)
                   logged_result=serviceCall.retrieve("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"0","acquireJobs")
                   self.assertEqual(logged_result[0],job_result)
                   serviceCall.log("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"releaseAllocation",\
                       ["prodAgentID","myRequest/1"],\
                       True)
                   logged_result=serviceCall.retrieve("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"0","releaseAllocation")
                   self.assertEqual(logged_result[0],True)
                   serviceCall.log("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"releaseJob",\
                       ["prodAgentID","jobspec1",30],\
                       True)
                   logged_result=serviceCall.retrieve("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"0","releaseJob")
                   self.assertEqual(logged_result[0],True)
                   try:
                       serviceCall.log("DN-ProdAgent-"+str(i)+"-Component-1","acquireAllocation",\
                          ["prodAgentID","myRequest",15],\
                          allocation_result)
                   except:
                       print("Handling error")
                       pass
           print("Pretending the client crashes")
           for i in xrange(0,self.prodagents):
               for j in xrange(0,self.components):
                   serviceCall.remove("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"acquireAllocation")
                   serviceCall.remove("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"acquireJobs")
                   serviceCall.remove("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"releaseAllocation")
                   serviceCall.remove("DN-ProdAgent-"+str(i)+"-Component-"+str(j),"releaseJob")
       except StandardError, ex:
           raise
           msg = "Failed :\n"
           msg += str(ex)
           self.fail(msg)


   def runTest(self):
       self.testA()

if __name__ == '__main__':
    import ProdCommon.Core.Initialize 
    unittest.main()

    
