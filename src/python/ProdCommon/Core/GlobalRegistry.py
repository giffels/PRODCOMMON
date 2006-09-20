#!/usr/bin/env python
"""
_GlobalRegistry_

GlobalRegistry Module to dynamically load plugins. The registry class
functions as a meta registry. It is responsible for handling 
the multiple registries within the production code, to allow
duplicated handler/plugin names through registry separations (e.g.
name spaces)

A handler/plugin implementation must be registered with a unique name
with this registry by doing:

from ProdCommon.Core.GlobalRegistry import registerHandler
registerHandler(objectRef, name,registry)

Where objectRef is a callable object and name is the name that
it will be registered with. registry is the registry (or namespace)
to which it will be registered.

"""
import logging

from ProdCommon.Core.ProdException import ProdException

class GlobalRegistry:
   """
   _GlobalRegistry_

   Static Class that is used to contain the map handler/plugin objects to
   handler/plugin name. The class level object provides singleton like 
   behaviour
   
   """
   registries= {}

   def __init__(self):
       msg = "ProdCommon.Core.GlobalRegistry should not be initialised"
       raise ProdException(msg,1003)

def registerHandler(objectRef, objectName,registryName):
   """
   _registerHandler_

   Register a new Handler with the name provided

   """
   if not GlobalRegistry.registries.has_key(registryName):
       logging.debug("Creating registry: "+registryName)
       GlobalRegistry.registries[registryName]={}

   if objectName in GlobalRegistry.registries[registryName].keys():
       msg = "Duplicate Name used to registerHandler object:\n"
       msg += "%s already exists\n" % objectName
       raise ProdException(msg,1004)
   if not callable(objectRef):
       msg = "Object registered as a Handler is not callable:\n"
       msg += "Object registered as %s\n" % objectName
       msg += "The object must be a callable object, either\n"
       msg += "a function or class instance with a __call__ method\n"
       raise ProdException(msg,1005)

   GlobalRegistry.registries[registryName][objectName] = objectRef
   logging.debug("Registered "+objectName+" in registry "+registryName)

   return

def retrieveHandler(objectName,registryName):
    """
    _retrieveHandler_

    Get the Handler object mapped to the name provided

    """
    if not GlobalRegistry.registries.has_key(registryName):
       msg = "Registry %s is not registered in the GlobalRegistry\n" % registryName
       raise ProdException(msg,1006)
    if objectName not in GlobalRegistry.registries[registryName].keys():
        msg = "Name: %s not a registered Handler\n" % objectName
        msg += "No object registered with that name in GlobalRegistry"
        raise RuntimeError, msg
    logging.debug("Retrieving "+objectName+" from registry "+registryName)
    return GlobalRegistry.registries[registryName][objectName] 

