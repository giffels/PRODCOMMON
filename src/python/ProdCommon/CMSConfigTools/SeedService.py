#!/usr/bin/env python
"""
_SeedService_

Tools for interacting with the RandomSeedService in the configuration


"""


import random
from random import SystemRandom
_inst  = SystemRandom()

#  //
# // Max bit mask size for 32 bit integers
#//  This is required to be max 900M for CMSSW
#_MAXINT = 2147483645
_MAXINT = 900000000

_SvcName = "RandomNumberGeneratorService"

def hasService(cfgInterface):
    """
    _hasService_

    Check to see wether the RandomSeedService is present in the cfg

    """
    return _SvcName in cfgInterface.cmsConfig.serviceNames()


def randomSeed():
    """
    Create a random 32 bit integer using the
    system time as a seed
    """
    try:
        value =  _inst.randint(1, _MAXINT)
    except:
        print "**** warning no system random generator seeds may not be very random "
        value =  random.randint(1, _MAXINT)
        
    return value


def seedsRequired(cfgInterface):
    """
    _seedsRequired_

    return the number of seeds required by counting the entries in the
    service entry in the cfg file

    structure is:

    service = RandomNumberGeneratorService
    {
      untracked uint32 sourceSeed = 987654321  # single source seed
      PSet moduleSeeds =
      {
         untracked uint32 Seed1 = 123456789
         ...
         untracked uint32 SeedN = 123456789
      }
    }

    So total seeds will be N + 1

    """
    if not hasService(cfgInterface):
        return 0
    seedSvc = cfgInterface.cmsConfig.service(_SvcName)
    #  //
    # // Source Seed present?
    #//
    hasSourceSeed = False
    if seedSvc.has_key("sourceSeed"):
        hasSourceSeed = True

    #  //
    # // Count module seeds
    #//
    moduleSeeds = seedSvc.get("moduleSeeds", ('PSet', 'tracked', {}))
    moduleSeedCount = len(moduleSeeds[2].keys())

    if hasSourceSeed:
        moduleSeedCount += 1
    return moduleSeedCount



def insertSeeds(cfgInterface, *seeds):
    """
    _insertSeeds_

    Insert the seeds provided into the cfgInterface

    number of seeds provided must at least equal the seedsRequired value
    for the cfgInterface.

    First entry in seeds will be used for the sourceSeed

    """
    seeds = list(seeds)
    seedsReq = seedsRequired(cfgInterface)
    if seedsReq == 0:
        return
    if len(seeds) < seedsReq:
        msg = "Not enough random seeds provided for cfg:"
        msg += "Number of seeds required: %s\n" % seedsReq
        msg += "Number of seeds provided: %s\n" % len(seeds)
        raise RuntimeError, msg

    
    sourceSeed = seeds[0]
    seeds.pop(0)
    seedSvc = cfgInterface.cmsConfig.service(_SvcName)
    
    #  //
    # // insert Source Seed
    #//
    seedSvc["sourceSeed"] = ('uint32', 'untracked', str(sourceSeed) )
    
    #  //
    # // insert module seeds
    #//
    moduleSeeds = seedSvc.get("moduleSeeds", ('PSet', 'tracked', {}))
    for key in moduleSeeds[2].keys():
        moduleSeeds[2][key] = ('uint32', 'untracked', str(seeds.pop(0) ) )

    return


def generateSeeds(cfg):
    """
    _generateSeeds_

    Find the number of seeds required for the cfg, and generate them using
    the system random device

    This is an interim solution, and should be replaced by seeds provided
    from the ProdMgr

    """
    #  //
    # // number of seeds required
    #//
    seedsReq = seedsRequired(cfg)

    #  //
    # // generate required number of seeds
    #//
    seeds = []
    for i in range(0, seedsReq):
        seed = randomSeed()
        seeds.append(seed)

    #  //
    # // insert generated seeds
    #//
    insertSeeds(cfg, *seeds)
    return
