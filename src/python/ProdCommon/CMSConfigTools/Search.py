#!/usr/bin/env python
"""
_Search_

Tools for searching for parameters in a CfgInterface object


Sample Searches:

------------------------------
In a source:

proc TEST {
   source = PythiaSource {
      PSet PythiaParameters = {
          int32 Pydatr_mrpy = 123456789
      }
}

search = PSetSearch()
search['Module'] = "PythiaSource"
search['PSets'] = ["PythiaParameters"]
search['Parameter'] = "Pydatr_mrpy"

Setting the seed using this search instance:

search.insertValue(cfgInterfaceInstance, 987654321)


---------------------------------

Nested PSets

proc TEST {
   module X = SomeModule {
      PSet Top = {
          PSet Nested = {
              int32 ParamName = 123
          }
      }
}

search = PSetSearch()
search['Module'] = "SomeModule"
search['PSets'] = ["Top", "Nested"]
search['Parameter'] = "ParamName"

---------------------------------

module level parameter

proc TEST {
   module X = SomeModule {
      int32 ParamName = 123
   }
}

search = PSetSearch()
search['Module'] = "SomeModule"
search['PSets'] = []
search['Parameter'] = "ParamName"

"""

def hasModule(cfgInt, moduleType):
    """
    _hasModule_

    Util method for finding a module based on its type.
    Searches both modules and the main input source.

    If the name is not found, None is returned, else a reference
    to the module dict is returned by reference.

    """
    for module in cfgInt.cmsConfig.psdata['modules'].values():
        modLabel = module['@classname'][2]
        if modLabel == moduleType:
            return module
    
    inpSrcName = cfgInt.cmsConfig.psdata['main_input']["@classname"][2]
    if moduleType == inpSrcName:
        return cfgInt.cmsConfig.psdata['main_input']
    
    return None


def findPSets(moduleDict, *psets):
    """
    _findPSets_

    Search a module level dictionary structure for a nested set of
    PSet dictionarys.

    return the matching PSet dictionary if found, else return None.

    """
    topDict = moduleDict
    for item in psets:
        if topDict.has_key(item):
            psetTuple = topDict[item]
            topDict = psetTuple[2]
        else:
            return None
    return topDict



    

class PSetSearch(dict):
    """
    _PSetSearch_

    Search object to contain a search in a PSet.

    The Search parameters consist of three fields:

    - *Module* : The module type (Eg PythiaSource, OscarProducer etc)

    - *PSets* : List of nested PSet names containing the parameter. Order is same as nesting order in the cfg. Topmost first

    - *Parameter* : The name of the parameter being searched for
    
    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Module", None)
        self.setdefault("PSets", [])
        self.setdefault("Parameter", None)


    def __call__(self, cfgInterface):
        """
        _operator()_

        Act on a CfgInterface object to perform a search based on
        this objects settings.

        If the parameter is found, a reference to the PSet dict containing
        it is returned (IE its parent dict, that will have a key matching
        the Parameter setting of this object

        """
        modRef = hasModule(cfgInterface, self["Module"])
        if modRef == None:
            return None
        pset = findPSets(modRef, *self['PSets'])
        if pset == None:
            return None
        param = pset.get(self['Parameter'], None)
        if param == None:
            return None
        return pset

    
    def insertValue(self, cfgInterface, newValue):
        """
        _insertValue_

        Search CfgInterface for a parameter matching this searches settings
        and, if found, insert the new value provided.
        
        """
        result =  self(cfgInterface)
        if result != None:
            current = result[self['Parameter']]
            result[self['Parameter']] = (current[0], current[1], newValue)
            return True
        return False
    
    
        

