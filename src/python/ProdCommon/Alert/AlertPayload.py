#!/usr/bin/env python
"""
_AlertPayload_

Object representing a Alert

"""
import os
import pickle
from time import strftime
from ProdCommon.MCPayloads.UUID import makeUUID
from ProdAgentCore.Configuration import ProdAgentConfiguration

class AlertPayload(dict):
    """
    _AlertPayload_

    Object representing a Alert generated be PA components

    Severity : MinorAlert, WarningAlert, ErrorAlert, CriticalAlert
    Component : ProdAgent Component Name (ID?)
    Message : Alert message
    Time : Alert creation time
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("Severity", None)
        self.setdefault("Component", None)
        self.setdefault("Message", None)
        self.setdefault("Time", strftime("%Y-%m-%d %H:%M:%S"))
        self.setdefault('FileName', None)
        self.update(**args)  

    def save(self):
        """
        _save_

        Pack the data object into an IMProvNode

        """
        output = None
        try:
            try:
                config = os.environ.get("PRODAGENT_CONFIG", None)
                if config == None:
                   msg = "No ProdAgent Config file provided\n"
                   raise RuntimeError, msg

                cfgObject = ProdAgentConfiguration()
                cfgObject.loadFromFile(config)
                alertHandlerConfig = cfgObject.get("AlertHandler")
                workingDir = alertHandlerConfig['ComponentDir']  


                dir = os.path.join(os.path.expandvars(workingDir),'Alerts')

                if not os.path.exists(dir): 
                   os.makedirs(dir)
                self.FileName = os.path.join(dir,"alert-%s.dat" %makeUUID())
                output = open(self.FileName, 'wb')
                pickle.dump(self, output)
            except Exception, ex:
                # to do: Exception handling
                print ex
                raise RuntimeError, str(ex)
        finally:
            if output:
                output.close()
        return


    def load(self, fileName):
        """
        _load_

        Unpack the pickled data object from the IMProvNode.
        Will keep the raw pickled data if there is a problem

        """
        pickledFile = None
        
        try:
            try:
                pickledFile = open(fileName, 'rb')
                alertPayload = pickle.load(pickledFile)
                self.update(**alertPayload)
            except:
                # to do: Exception handling
                raise RuntimeError
        finally:
            if pickledFile:
                pickledFile.close()   
        return
        
        
if __name__ == "__main__":
    alert = AlertPayload()
    alert["Severity"] = "Warning"
    alert["Component"] = "Test"
    alert["Message"] = "Test Message"
    
    alert.save()
    
    print alert
    
    alert1 = AlertPayload()
    alert1.load(alert.FileName)
    
    print alert1
