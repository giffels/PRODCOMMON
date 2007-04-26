#!/usr/bin/env python
"""
_MaxEvents_

Interface to maxEvents PSet for controlling how output is handled


"""

import FWCore.ParameterSet.Types as CfgTypes
from ProdCommon.CMSConfigTools.ConfigAPI.Utilities import _CfgNoneType


    

class MaxEvents:
    """
    _MaxEvents_

    Object API for manipulating the maxEvents PSet

    """
    def __init__(self, psetRef):
        self.data = psetRef
        

    def __str__(self):
        return self.data.dumpConfig()



    def input(self):
        """
        _input_

        Get limit on number of input maxEvents for source

        """
        cfgType = getattr(self.data, "input", _CfgNoneType())
        return cfgType.value()


    def setMaxEventsInput(self, maxEv):
        """
        _setMaxEventsInput_

        Set the input maxEvents limit for the source

        """
        self.data.input = CfgTypes.untracked(CfgTypes.int32(maxEv))
        return

    def perModuleLimit(self):
        """
        _perModuleLimit_

        Return True if this PSet contains per output module 
        settings for maxEvents, false if there is a single
        output limit
        """
        cfgType = getattr(self.data, "output", _CfgNoneType())
        if cfgType.value() == None:
            return False

        if cfgType.__class__.__name__ == "int32":
            # single limit for all modules
            return False
        # Should be a VPSet
        return True
        

    def setMaxEventsOutput(self, maxEv, moduleName = None):
        """
        _setMaxEventsOutput_
        """
        value =  CfgTypes.untracked(CfgTypes.int32(maxEv))
        
        if moduleName == None:
            self.data.output = value
            return

        cfgType = getattr(self.data, "output", None)
        if cfgType.__class__.__name__ == "VPSet":
            # is a VPset so we append to that
            self.data.output.append(
                CfgTypes.untracked( CfgTypes.PSet( moduleName = value) )
                )
        else:
            # add new VPset
            self.data.output = CfgTypes.untracked(
                CfgTypes.VPSet(
                CfgTypes.untracked(CfgTypes.PSet( moduleName = value))
                )
                )
        return

    
    def parameters(self):
        """
        _parameters_

        reduce to a map of parameters: value.

        input : value
        output : value
        outmodname : value

        """
        inputVal = getattr(self.data, "input", _CfgNoneType()).value()
        outputVal = getattr(self.data, "output", _CfgNoneType()).value()
        result = {}
        result['input'] = inputVal
        result['output'] = None
        if type(outputVal) == type([]):
            # Vector of PSets
            for x in outputVal:
                name = x.parameterNames_()[-1]
                value = getattr(x, name).value()
                result[name] = value
        elif type(outputVal) == type(int(1)):
            # integer
            result['output'] = outputVal
            
        return result
            
        
        
