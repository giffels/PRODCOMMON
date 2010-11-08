#!/usr/bin/env python
"""
_ModifyJobReport.py

Example of how to use the FwkJobRep package to update a job report post processing


"""
try:
    import json
except:    
    import simplejson as json
import os, string
import sys
from subprocess import Popen, PIPE, STDOUT

from ProdCommon.FwkJobRep.ReportParser import readJobReport


def readCksum(filename):
    """
    _readCksum_

    Run a cksum command on a file an return the checksum value

    """

    pop = Popen(["cksum", "%s" % filename],
                stdout = PIPE, stderr = STDOUT)
    output = pop.communicate()[0]
    if pop.wait():
        return None
    value = output.split()[0]
    print "checksum = ", value
    return value


def fileSize(filename):
    """
    _fileSize_

    Get size of file

    """
    print "(size) os.stat(filename)[6] = ", os.stat(filename)[6]
    return os.stat(filename)[6]    
    

def addFileStats(file):
    """
    _addFileStats_

    Add checksum and size info to each size

    """
    file['Size'] = fileSize(file['PFN'])
    checkSum = readCksum(file['PFN'])
    file.addChecksum('cksum',checkSum)
    return


def modifyFile(f, file_name, for_file):
    """
    _modifyFile_
    
    Calls functions to modify PFN and LFN
    """
    print "    file = ", file_name
    newPfn = for_file['endpoint'] + file_name
    print "    newPfn = ", newPfn

    newLfn = for_file['for_lfn'] + file_name
    print "    newLfn = ", newLfn


    updatePFN(f, f['LFN'], newPfn)

    updateLFN(f, f['LFN'], newLfn)
    
    if (for_file['surl_for_grid'] != ""):
        newSurlForGrid = for_file['surl_for_grid'] + file_name  
    else:     
        newSurlForGrid = f['PFN']  
           
    updateSurlForGrid(f, newSurlForGrid)

    return


def updateSurlForGrid(f, newSurlForGrid):
    """
    _updateSurlForGrid_

    """
    f['SurlForGrid'] = newSurlForGrid
    return

def updatePFN(f, lfn, newPFN):
    """
    _updatePFN_

    Update a PFN for an LFN, based on a stage out to some SE.
    """
    if f['LFN'] != lfn:
        return

    f['PFN'] = newPFN
    f['SEName'] = for_file['se_name']
    return


def updateLFN(f, lfn, newLFN):
    """
    _updateLFN_

    Update a LFN.
    """
    if f['LFN'] != lfn:
        return
    f['LFN'] = newLFN
    return


if __name__ == '__main__':

    # Example:  Load the report, update the file stats, pretend to do a stage out
    # and update the information for the stage out

############################
### file_list, se_path, se_name, for_lfn, UserProcessedDataset have been passed with json file
### json:
### {"file_1.root": {"se_name": "se_legnaro", "for_lfn": "/store/xxx/yyy", "se_path": "srm=se_legnaro/xxx/yyy", "UserProcessedDataset": "$USER_processedDataset2_$PSETHASH"}, "file_2": {"se_name": "se_legnaro", "for_lfn": "/store/zzz/www", "se_path": "srm:se_bari/zzz/www", "UserProcessedDataset": "$USER_processedDataset2_$PSETHASH"}}

### instead of:
### $RUNTIME_AREA/ProdCommon/FwkJobRep/ModifyJobReport.py fjr $RUNTIME_AREA/crab_fjr_$NJob.xml n_job $NJob for_lfn $FOR_LFN PrimaryDataset $PrimaryDataset  ApplicationFamily $ApplicationFamily ApplicationName $executable cmssw_version $CMSSW_VERSION psethash $PSETHASH se_name $SE se_path $SE_PATH file_list $file_list UserProcessedDataset $USER-$ProcessedDataset-$PSETHASH
############################

    L = sys.argv[1:]
    diz={}
    
    i = 0
    while i < len(L):
        diz[L[i]] = L[i+1]
        i = i + 2

    if diz.has_key('json'):
        json_file = diz['json']
        fp = open(json_file, "r")
        inputDict = json.load(fp)
        fp.close()
        print "inputDict = ", inputDict
    else:
        print "Error: no json file provided"
        sys.exit()
        
    if diz.has_key('fjr'):
        inputReport = diz['fjr']
        reports = readJobReport(inputReport)
    
        # report is an instance of FwkJobRep.FwkJobReport class
        # can be N in a file, so a list is always returned
        # by for readJobReport, here I am assuming just one report per file for simplicity
        try:   
            report = reports[-1]
        except IndexError:
            print "Error: fjr file does not contain enough information"
            sys.exit(1)
    else:
        print "Error: no fjr provided"
        sys.exit(1)


    # ARGs parameters
    if diz.has_key('n_job'):
        n_job = diz['n_job'] 
    else:
        print "Error: it is necessary to specify the job number" 
        sys.exit(1)
        
    ##### filter name takes from environment variable #####
    try:
       var_filter = os.getenv('var_filter')
    except:
       var_filter=''
    diz_filter={}
    if (var_filter):
       diz_filter=json.loads(var_filter)
       print "diz_filter = ", diz_filter
    else:
        print "no filter to add to ProcessedDataset"

    if diz.has_key('UserProcessedDataset'): 
        UserProcessedDataset = diz['UserProcessedDataset']
    else:
        UserProcessedDataset=''
    print "UserProcessedDataset = ", UserProcessedDataset
    
    #### Adding AnalysisFile ####
    if (len(report.files) == 0) and (len(report.analysisFiles) == 0):
       print "Warninig: no EDM_output file or NO_EDM_output to modify in the fjr"
       print "Adding a no EDM_output file"
       for file_name in inputDict.keys():
           for_file = inputDict[file_name]
           report.newAnalysisFile()
           for aFile in report.analysisFiles:
               if (aFile['SEName'] == None):
                   aFile['SEName']=for_file['se_name']
               if (aFile['LFN'] == None):    
                   aFile['LFN']=for_file['for_lfn']+os.path.basename(file_name)
               if (aFile['PFN'] == None):    
                   aFile['PFN']=for_file['endpoint']+os.path.basename(file_name)
               ### FEDE FOR COPY DATA     
               if (aFile['SurlForGrid'] == None):    
                   aFile['SurlForGrid']=for_file['surl_for_grid']+os.path.basename(file_name)
               ####     
           report.save()
       report.write("NewFrameworkJobReport.xml")         
    else:
        if (len(report.files) != 0):
            ### f is <file> tag
            for f in report.files:
                ### f['PFN'] is the original file name, as produced by CMSSW: out.root o file:out.root
                if (string.find(f['PFN'], ':') != -1):
                    tmp_path = string.split(f['PFN'], ':')
                    f['PFN'] = tmp_path[1]
                    
                ### check if the file exists in the wn
                if not os.path.exists(f['PFN']):
                    print "Error: Cannot find file: %s " % f['PFN']
                    sys.exit(1)
                    
                for file_name in inputDict.keys():
                    print "file_name in inputDict = ", file_name
                    ### file is the name of produced file + number submission: out_6_1.root
                    only_name = os.path.basename(file_name)
                    tmp = string.split(only_name, ".")
                    suff = '.' + tmp[-1]
                    tmp = string.split(only_name, "_"+n_job)
                    pref = tmp[0]
                    name = pref + suff
                    if ( name == os.path.basename(f['PFN'])):
                       ### parameter for this file extracted from json 
                       for_file = inputDict[file_name]
                       #Generate per file stats
                       addFileStats(f)

                       datasetinfo=f.newDataset()
                       datasetinfo['PrimaryDataset'] = diz['PrimaryDataset'] 
                       datasetinfo['DataTier'] = "USER" 
                       datasetinfo['ApplicationFamily'] = diz['ApplicationFamily'] 
                       datasetinfo['ApplicationName'] = diz['ApplicationName'] 
                       datasetinfo['ApplicationVersion'] = diz['cmssw_version'] 
                       datasetinfo['PSetHash'] = diz['psethash']
                       datasetinfo['PSetContent'] = "TOBEADDED"
                       #########################################################################
                       datasetinfo['ProcessedDataset'] = UserProcessedDataset
                       if diz_filter.has_key(name):
                           filter = diz_filter[name]
                           print "filter = ", filter
                           if (filter):
                               FilterUserProcessedDataset = UserProcessedDataset + '-' + str(filter)
                               datasetinfo['ProcessedDataset'] = FilterUserProcessedDataset 
                       #########################################################################    
                       ### to check if the job output is composed by more files
                       modifyFile(f, os.path.basename(file_name), for_file)    
                    else:
                        print "file not found "
                        continue

        if (len(report.analysisFiles) != 0):
            for aFile in report.analysisFiles:
                aFile['PFN'] = os.path.basename(aFile['FileName'])
                for file_name in inputDict.keys():
                    only_name = os.path.basename(file_name)
                    tmp = string.split(only_name, ".")
                    suff = '.' + tmp[-1]
                    tmp = string.split(only_name, "_"+n_job)
                    pref = tmp[0]
                    name = pref + suff
                    if ( name == aFile['PFN']):
                        for_file = inputDict[file_name]
                        modifyFile(aFile, os.path.basename(file_name), for_file)
                    
        # After modifying the report, you can then save it to a file.
        report.write("NewFrameworkJobReport.xml")
    


