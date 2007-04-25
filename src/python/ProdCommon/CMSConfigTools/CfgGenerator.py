#!/usr/bin/env python
"""
_CfgGenerator_

Object that can be used to take a config file and use it to generate
job specific configuration files from it

"""

from ProdCommon.CMSConfigTools.CfgInterface import CfgInterface
from ProdCommon.CMSConfigTools.JobReport import checkJobReport
from ProdCommon.CMSConfigTools.SeedService import generateSeeds



class CfgGenerator:
    """
    _CfgGenerator_

    Job Specific Cfg File generator that generates a job specific cfg
    using the input cfg as a template

    
    """
    def __init__(self, pyFormatCfgFile, isString = False, appControls = {}):
        self.template = CfgInterface(pyFormatCfgFile, isString)
        self.appControls = appControls
        #  //
        # // Ensure that the template contains the expected 
        #//  framework job report entry
        checkJobReport(self.template)
        

        

    def __call__(self, jobName, **args):
        """
        _operator()_

        Create and return a new CfgInteface instance with the
        jobname provided inserted into it.

        Each outputModule will get modified to use the JobName
        in the fileName, lfn and catalog fields.

        Keyword args are used to insert information into the
        inputSource.

        Supported keyword args are:

        maxEvents - maxEvents parameter to input source (int32)
        firstRun - firstRun number parameter to input source (int32)
        fileNames - Input file names to be used, list expected
        
        """
        #  //
        # // Create the new config object
        #//
        newCfg = self.template.clone()
        

        #  //
        # // Output modules first, use the module name in the 
        #//  parameters in case of multiple modules
        #  //Should be done with Dataset info in future
        # //
        #//
        for modName, outModule in newCfg.outputModules.items():
            outModule.setCatalog("%s-%s-Output.xml" % (jobName, modName))
            outModule.setFileName("%s-%s.root" % (jobName, modName))
            outModule.setLogicalFileName("%s-%s.root" % (jobName, modName))


        maxEvents = args.get("maxEvents", None)

        if maxEvents != None:
            selectionEff = self.appControls.get("SelectionEfficiency", None)
            evMultiplier = self.appControls.get("EventMultiplier", None)
            perRunFrac = self.appControls.get("PerRunFraction", None)
            
            

            #  //
            # // Adjust number of events for selection efficiency
            #//
            if selectionEff != None:
                newMaxEv = float(maxEvents) / float(selectionEff)
                maxEvents = int(newMaxEv) 

            #  //
            # // If this node has an Event Multiplier, adjust maxEvents
            #//
            if evMultiplier != None:
                maxEvents = int(maxEvents) * int(evMultiplier)

            #  //
            # // Per Run Fraction means generator will run in two runs
            #//  
            if perRunFrac != None:
                numEvInRun = float(maxEvents) * float(perRunFrac)
                numEvInRun = int(numEvInRun)
                maxEvents = -1
                newCfg.inputSource.setNumberEventsInRun(numEvInRun)

            #  //
            # // Insert whatever maxEvents turned out to be
            #//
            newCfg.inputSource.setMaxEvents(maxEvents)
            newCfg.hackMaxEvents(maxEvents)
            
        skipEvents = args.get("skipEvents", None)
        if skipEvents != None:
            newCfg.inputSource.setSkipEvents(skipEvents)

        firstRun = args.get("firstRun", None)
        if firstRun != None:
            newCfg.inputSource.setFirstRun(firstRun)

        fileNames = args.get("fileNames", None)
        if fileNames != None:
            newCfg.inputSource.setFileNames(*fileNames)

            
            
        #  //
        # // Insert Random seeds into the random seed service
        #//
        generateSeeds(newCfg)
        

        
            
        #  //
        # // Return the new CfgInterface containing the new config
        #//
        return newCfg


    def __str__(self):
        """string rep: dump python format of template cfg"""
        return str(self.template)
