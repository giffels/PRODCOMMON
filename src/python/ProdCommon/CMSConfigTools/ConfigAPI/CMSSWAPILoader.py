#!/usr/bin/env python
"""
Utility to dynamically load the CMSSW python API for some version,
work with it and then remove it so that you can work with multiple
CMSSW versions APIs


"""

import os
import sys



class CMSSWAPILoader:
    """
    _CMSSWAPILoader_

    Object that provides a session like interface to the python
    modules in a CMSSW release without having to go through a scram
    setup.

    Initialise with:
    - Scram Architecture value
    - CMSSW Version
    - Value of CMS_PATH if not already set in os.environ

    This provides you with an API to load the Python tools for working
    with cfg files in several releases, and cleanup of the imports
    used.

    Example:

    
    loader = CMSSWAPILoader("slc3_ia32_gcc323", "CMSSW_1_3_1",
                        "/uscmst1/prod/sw/cms/"
                        )

    try:
       import FWCore
    except ImportError:
       print "Cant import FWCore"

    # Make release available
    loader.load()
    
    # now have modules available for import
    import FWCore.ParameterSet.parseConfig as Parser
    cmsCfg = Parser.parseCfgFile(cfg)

    # finished with release, clean up
    loader.unload()

    try:
        import FWCore
    except ImportError:
        print "Cant import FWCore"
    

    """

    def __init__(self, arch, version, cmsPath = None):
        self.loaded = False
        if cmsPath == None:
            cmsPath = os.environ.get('CMS_PATH', None)
        if cmsPath == None:
            msg = "CMS_PATH is not set, cannot import CMSSW python cfg API"
            raise RuntimeError, msg

        self.cmsPath = cmsPath
        self.arch = arch
        self.version = version
        self.pythonLib = os.path.join(self.cmsPath, self.arch,
                                      "cms", "cmssw", self.version ,"python")
        
        if not os.path.exists(self.pythonLib):
            msg = "Unable to find python libs for release:\n"
            msg += "%s\n" % self.pythonLib
            msg += " CMS_PATH=%s\n Architecture=%s\n" % (
                self.cmsPath, self.arch)
            msg += " Version=%s\n" % self.version

        searchPaths = ["/", # allow absolute cfg file paths
            os.path.join(self.cmsPath, self.arch, "cms", "cmssw" ,
                         self.version, "src"),
            os.path.join(self.cmsPath, self.arch, "cms", "cmssw" ,
                         self.version, "share"),
            ]
        self.cmsswSearchPath = ":".join(searchPaths)
        

    def load(self):
        """
        _load_

        Add the python lib to sys.path and test the import
        of the libraries

        """
        sys.path.append(self.pythonLib)
        try:
            import FWCore.ParameterSet
        except Exception, ex:
            msg = "Error importing FWCore.ParameterSet modules:\n"
            msg += "%s\n" % str(ex)
            raise RuntimeError, msg
        os.environ['CMSSW_SEARCH_PATH'] = self.cmsswSearchPath
        self.loaded = True
        return
        
    def unload(self):
        """
        _unload_

        Delete module references and remove api from the sys.path

        """
        sys.path.remove(self.pythonLib)
        for modName in sys.modules.keys():
            if modName.startswith("FWCore"):
                sys.modules.pop(modName)
        os.environ.pop("CMSSW_SEARCH_PATH")
        self.loaded = False
        return
        
    


