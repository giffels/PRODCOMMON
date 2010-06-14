#!/usr/bin/env python
"""
Utility to dynamically load the CMSSW python API for some version,
work with it and then remove it so that you can work with multiple
CMSSW versions APIs


"""

import os
import sys
import imp
import __builtin__

class RollbackImporter:
    """
    _RollbackImporter_

    Safe way to clean up FWCore modules when they are imported

    """
    def __init__(self):
        "Creates an instance and installs as the global importer"
        self.previousModules = sys.modules.copy()
        self.realImport = __builtin__.__import__
        __builtin__.__import__ = self._import
        self.newModules = {}
        
    def _import(self, name, globals=None, locals=None, fromlist=[], level=None):
        # level not used in python2.3
        if level is not None:
            result = apply(self.realImport, (name, globals, locals, fromlist, level))
        else:
            result = apply(self.realImport, (name, globals, locals, fromlist))
        self.newModules[name] = 1
        return result
        
    def uninstall(self):
        for modname in self.newModules.keys():
            if not self.previousModules.has_key(modname):
                # Force reload when modname next imported
                if sys.modules.has_key(modname):
                    del(sys.modules[modname])

        #
        # Safety net for FWCore modules
        #
        for modname in sys.modules.keys():
            if modname.startswith("FWCore"):
                del sys.modules[modname]
                    
                    
        __builtin__.__import__ = self.realImport
        

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
        self.isPatch = False
        if cmsPath == None:
            cmsPath = os.environ.get('CMS_PATH', None)
        if cmsPath == None:
            msg = "CMS_PATH is not set, cannot import CMSSW python cfg API"
            raise RuntimeError, msg
        self.cmsPath = cmsPath
        self.arch = arch
        self.version = version
        self.paths = []
        
        if self.version.find("patch") > -1:
            self.isPatch = True

            
        cmsswDir = "cmssw"
        if self.isPatch:
            cmsswDir = "cmssw-patch"

        
        self.releaseBase = os.path.join(self.cmsPath, self.arch,
                                        "cms", cmsswDir, self.version)
            
        self.pythonLib = "%s/python" % self.releaseBase

        self.envFile = "%s/cmsswPaths.py" % self.pythonLib
    
        
        if not os.path.exists(self.pythonLib):
            msg = "Unable to find python libs for release:\n"
            msg += "%s\n" % self.pythonLib
            msg += " CMS_PATH=%s\n Architecture=%s\n" % (
                self.cmsPath, self.arch)
            msg += " Version=%s\n" % self.version
            raise RuntimeError, msg

        self.paths.append(self.pythonLib)

        if os.path.exists(self.envFile):
            fp, pathname, description= imp.find_module(
                os.path.basename(self.envFile).replace(".py", ""),
                [os.path.dirname(self.envFile)])
            modRef = imp.load_module("AutoLoadCMSSWPathDefinition", fp, pathname, description)
            pythonPaths = getattr(modRef, "cmsswPythonPaths", None)
            if pythonPaths != None:
                self.paths.extend(pythonPaths)
        else:
            if self.isPatch:
                msg = "Patch release in use but doesnt have environment definition file:\n"
                msg += "%s\n" % self.envFile
                msg += "Cannot proceed with patch release..."
                raise RuntimeError, msg
            
        searchPaths = ["/", # allow absolute cfg file paths
            os.path.join(self.cmsPath, self.arch, "cms", "cmssw" ,
                         self.version, "src"),
            os.path.join(self.cmsPath, self.arch, "cms", "cmssw" ,
                         self.version, "share"),
            ]
        self.cmsswSearchPath = ":".join(searchPaths)
        
        self.rollbackImporter = None

    def load(self):
        """
        _load_

        Add the python lib to sys.path and test the import
        of the libraries

        """
        self.rollbackImporter = RollbackImporter()
        sys.path.extend(self.paths)
        try:
            import FWCore.ParameterSet
        except Exception, ex:
            msg = "Error importing FWCore.ParameterSet modules:\n"
            msg += "%s\n" % str(ex)
            sys.path.remove(self.pythonLib)
            self.rollbackImporter.uninstall()
            self.rollbackImporter = None
            raise RuntimeError, msg
        os.environ['CMSSW_SEARCH_PATH'] = self.cmsswSearchPath
        os.environ['CMSSW_VERSION'] = self.version
        self.loaded = True
        return
        
    def unload(self):
        """
        _unload_

        Delete module references and remove api from the sys.path

        """
        for pathname in self.paths:
            sys.path.remove(pathname)
        os.environ.pop("CMSSW_SEARCH_PATH")
        self.rollbackImporter.uninstall()
        self.rollbackImporter = None
        self.loaded = False
        if sys.modules.has_key('AutoLoadCMSSWPathDefinition'):
            del sys.modules['AutoLoadCMSSWPathDefinition']
        return
        
    


