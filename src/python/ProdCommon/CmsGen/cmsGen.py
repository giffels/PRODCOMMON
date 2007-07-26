#!/usr/bin/env python
import os,sys,getopt
import random
from FwkJobRep.FwkJobReport import FwkJobReport

"""
AlpGen generator
"""
def alpGen():
    """
    Generator dependent from here
    """
    #alpPath	 = "http://cmsdoc.cern.ch/cms/PRS/gentools/www/alpgen"
    #alpTgz	 = "AlpgenV205CMSDefault.tgz"
    #alpPath	 = "http://mlm.web.cern.ch/mlm/alpgen/V2.1"
    alpPath	 = "http://cern.ch/ceballos/alpgen/code"
    alpTgz	 = "v212.tgz"
    alpGridPath  = "http://cern.ch/ceballos/alpgen/psgrids"
    """
    Download and install alpgen
    """
    msg="wget %s/%s" % (alpPath, alpTgz)
    os.system(msg)
    msg="tar fzx  %s" % alpTgz
    os.system(msg)
    #msg="cd `echo alpgen*`; make validate"
    #os.system(msg)
    """
    Download and install alpgen grid (with option 1)
    """
    msg="echo Download and install alpgen grid"
    os.system(msg)

    # First line in the config file is the generator
    # Second line in the config file is the executable
    # Third line in the config file is the name of the grid file
    file      = open(cfgFile, 'r')
    generatorLine  = file.readline() # Not used
    executableLine = file.readline()
    executableString = executableLine.split('\n')
    gridLabelLine  = file.readline()
    alpGridLabel  = gridLabelLine.split('\n')
    global outputFile
    outputFile = "%s" % alpGridLabel[0]

    try:
        msg="wget %s/%s.grid2 -O %s.grid2" % (alpGridPath,alpGridLabel[0],alpGridLabel[0])
        os.system(msg)
    except Exception, ex:
    	return 1
    """
    Splitting the cfg file into two files
    """
    cfgFile_we = "%s-we" % cfgFile
    msg = "rm -f %s" % cfgFile_we
    os.system(msg)
    file_we = open(cfgFile_we, 'w')

    cfgFile_uw = "%s-uw" % cfgFile
    msg = "rm -f %s" % cfgFile_uw
    os.system(msg)
    file_uw = open(cfgFile_uw, 'w')

    msg =  "1	 ! imode\n"
    msg += "%s	 ! label for files\n" % alpGridLabel[0]
    msg += "2    ! start with: 0=new grid, 1=previous warmup grid, 2=previous generation grid\n"
    msg += "0 0  ! Nevents/iteration,  N(warm-up iterations)\n"
    msg += "%d	 ! Nevents generated after warm-up\n" % int(nEvents)
    msg += "*** The above 4 lines provide mandatory inputs for all processes\n"
    msg += "*** (Comment lines are introduced by the three asteriscs)\n"
    msg += "*** The lines below modify existing defaults for the hard process under study\n"
    file_we.write(msg)

    line=file.readline()
    while(line != ''):
    	 file_we.write(line)
    	 line=file.readline()

    numberOfSeedsNeeded = 4
    random.seed(int(seeds))
    seedsAlpgen = []
    for i in range(0, numberOfSeedsNeeded):
      seedsAlpgen.append(random.randint(99999,999999))
    msg =  "iseed1 %d\n" % seedsAlpgen[0]
    msg += "iseed2 %d\n" % seedsAlpgen[1]
    msg += "iseed3 %d\n" % seedsAlpgen[2]
    msg += "iseed4 %d\n" % seedsAlpgen[3]
    file_we.write(msg)

    msg =  "2	 ! imode\n"
    msg += "%s	 ! label for files\n" % alpGridLabel[0]
    msg += "*** The above 2 lines provide mandatory inputs for all processes\n"
    msg += "*** (Comment lines are introduced by the three asteriscs)\n"
    msg += "*** The lines below modify existing defaults for the hard process under study\n"
    file_uw.write(msg)

    file.close()
    file_we.close()
    file_uw.close()
    
    """
    Make weighted events
    """
    msg="echo ******Make weighted events******"
    os.system(msg)

    msg = "%s < %s" % (executableString[0],cfgFile_we)
    try:
    	os.system(msg)
    except Exception, ex:
    	return 2
    """
    Make unweighted events
    """
    msg="echo ******Make unweighted events******"
    os.system(msg)

    msg = "%s < %s" % (executableString[0],cfgFile_uw)
    try:
    	os.system(msg)
    except Exception, ex:
    	return 3
    
    """
    Making links and providing some information
    """
    try:
        msg = "rm -f nCmsGenEvents.txt;"
        msg+= "echo `tail -1 %s_unw.par | awk '{print$1}'` > nCmsGenEvents.txt;" % alpGridLabel[0]
        os.system(msg)
    except Exception, ex:
    	return 4

    """
    Links are not needed anymore in Alpgen

    try:
        msg = "cd ../cmsRun1; ln -s ../cmsGen1/%s_unw.par .;" % alpGridLabel[0]
        os.system(msg)
    except Exception, ex:
    	return 5
    try:
        msg = "cd ../cmsRun1; ln -s ../cmsGen1/%s.unw .;" % alpGridLabel[0]
        os.system(msg)
    except Exception, ex:
    	return 5
    try:
        msg = "cd ../cmsRun1; ln -s ../cmsGen1/%s.wgt .;" % alpGridLabel[0]
        os.system(msg)
    except Exception, ex:
    	return 5
    """

    return 0

"""
compHep generator
"""
def compHep():
    """
    Generator dependent from here
    """
    compHepPath	 = "http://cern.ch/ceballos/comphep"
    compHepTgz	 = "comphep-compiled.tgz"
    """
    Download and install compHep
    """
    try:
        msg="wget %s/%s" % (compHepPath, compHepTgz)
	os.system(msg)
	msg = "tar xvzf  %s;" % compHepTgz
	###msg+= "cd  Z2ll_bb-07; tar xvzf comphep-4.2p1.tgz;"
	###msg+= "cd comphep-4.2p1; ./configure; make; make setup;"
	os.system(msg)
	myCompHepDir = os.getcwd()+"/Z2ll_bb-07/comphep-4.2p1/END"
	compHepDir = ""
	myCompHep = myCompHepDir.split('/')
	i=0
	while(myCompHep[i] != "END"):
    	    if(myCompHep[i] != ""):
    		compHepDir+= "\/"
    		compHepDir+= myCompHep[i]
    	    i=i+1
    except Exception, ex:
    	return 6
    """
    Go to the working directory and untar production area
    """
    try:
    	msg = "cd Z2ll_bb-07/comphep_4.2p1_test/;"
    	###msg+= "cp ../Zbb_llbb_mass_prod.tar.gz .;"
    	###msg+= "cp ../num_batch.pl .;"
    	###msg+= "cp ../num_batch_v1.5.pl .;"
    	###msg+= "cp ../run-evnt.csh .;"
    	###msg+= "tar xvzf Zbb_llbb_mass_prod.tar.gz;"
    	os.system(msg)
    except Exception, ex:
    	return 6
    """
    Modify input parameters
    """
    try:
    	numberOfSeedsNeeded = 9
    	random.seed(int(seeds))
    	seedsCompHep = []
    	for i in range(0, numberOfSeedsNeeded):
    	  seedsCompHep.append(random.randint(99999999,999999999))
    	msg = "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999991/999%d/\" > batch.dat;" % seedsCompHep[0]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999992/999%d/\" > batch.dat;" % seedsCompHep[1]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999993/999%d/\" > batch.dat;" % seedsCompHep[2]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999994/999%d/\" > batch.dat;" % seedsCompHep[3]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999995/999%d/\" > batch.dat;" % seedsCompHep[4]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999996/999%d/\" > batch.dat;" % seedsCompHep[5]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999997/999%d/\" > batch.dat;" % seedsCompHep[6]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999998/999%d/\" > batch.dat;" % seedsCompHep[7]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	msg+= "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat | sed s\"/999999999999/999%d/\" > batch.dat;" % seedsCompHep[8]
    	msg+= "mv batch.dat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/batch.dat;"
    	os.system(msg)
    except Exception, ex:
    	return 6

    try:
        msg = "cat Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/n_comphep | sed s\"/MYCOMPHEPDIR/%s/\" > n_comphep;" % compHepDir
        msg+= "mv n_comphep Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/n_comphep;"
        msg+= "chmod a+x Z2ll_bb-07/comphep_4.2p1_test/Zbb_llbb_mass_prod/n_comphep*;"
        os.system(msg)
    except Exception, ex:
    	return 6

    """
    Run compHEP
    """
    try:
        msg = "echo Running compHEP;"
        msg+= "cd Z2ll_bb-07/comphep_4.2p1_test;"
        msg+= "./run-evnt.csh 00 %d;" % int(nEvents)
        os.system(msg)
    except Exception, ex:
    	return 2

    """
    Events mixing to produce *.PEV files
    """
    try:
        msg = "echo Events mixing to produce PEV files;"
      	msg+= "cd Z2ll_bb-07;"
      	#msg+= "tar xvzf cpyth-1.2.3.tgz;"
      	msg+= "cd cpyth-1.2.3;"
      	#msg+= "./configure;"
      	#msg+= "make;"
      	#msg+= "make install;"
      	msg+= "cd ../cpyth_1.2.3_test;"
      	msg+= "cp ../zbb-cross-section-correction.csh .;"
	msg+= "./zbb-cross-section-correction.csh 00 >& nCmsGenEvents.txt;"
        os.system(msg)
    except Exception, ex:
    	return 3

    """
    Making links and providing some information
    """
    try:
        msg = "rm -f nCmsGenEvents.txt;"
	msg+= "awk '{print$6}' Z2ll_bb-07/cpyth_1.2.3_test/nCmsGenEvents.txt > nCmsGenEvents.txt;"
        os.system(msg)
    except Exception, ex:
    	return 4

    try:
        msg = "ls -l Z2ll_bb-07/cpyth_1.2.3_test/;"
        msg+= "cat Z2ll_bb-07/cpyth_1.2.3_test/nCmsGenEvents.txt;"
        msg+= "cp Z2ll_bb-07/cpyth_1.2.3_test/Zbb-00.PEV .;"
        msg+= "cd ../cmsRun1; ln -s ../cmsGen1/Zbb-00.PEV .;"
        os.system(msg)
    except Exception, ex:
    	return 5

    global outputFile
    outputFile = "Zbb-00.PEV"

    return 0

def processFrameworkJobReport():
    """
    Runtime tool for processing the Framework Job Report produced
    by CMsGen executables

    """
    theJobReport = FwkJobReport()
    #
    # If everything worked well set the success status
    # else record a failure and error
    if jobFailed == 0:
       theJobReport.status = "Success"
       theJobReport.exitCode = 0
    elif jobFailed == 1:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11111
       errDetail = theJobReport.addError(11111, "CmsGenFailure")
       errDetail['Description'] = "Error retrieving grid file"
    elif jobFailed == 2:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11112
       errDetail = theJobReport.addError(11112, "CmsGenFailure")
       errDetail['Description'] = "Error producing step 1 events"
    elif jobFailed == 3:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11113
       errDetail = theJobReport.addError(11113, "CmsGenFailure")
       errDetail['Description'] = "Error producing step 2 events"
    elif jobFailed == 4:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11114
       errDetail = theJobReport.addError(11114, "CmsGenFailure")
       errDetail['Description'] = "Error computing the number of produced events"
    elif jobFailed == 5:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11115
       errDetail = theJobReport.addError(11115, "CmsGenFailure")
       errDetail['Description'] = "Error making a link"
    elif jobFailed == 6:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11116
       errDetail = theJobReport.addError(11116, "CmsGenFailure")
       errDetail['Description'] = "Error preparing the executation"
    else:
       theJobReport.status = "Failed"
       theJobReport.exitCode = 11117
       errDetail = theJobReport.addError(11117, "CmsGenFailure")
       errDetail['Description'] = "General cmsGen Failure"

    if jobFailed == 0:
      file        = open("nCmsGenEvents.txt", 'r')
      nEventsLine = file.readline()
      nEvents     = nEventsLine.split('\n')
      file.close()
      totalEvents = nEvents[0]
      outputFileUnw = ""
      if generator == "alpgen":
          outputFileUnw = "%s.unw" % outputFile
      elif generator == "comphep":
          outputFileUnw = "%s" % outputFile
 
      if not os.path.exists(outputFileUnw):
          msg = "Output file Not Found: %s" % outputFileUnw
          raise RuntimeError, msg
      totalSize   = os.stat(outputFileUnw)[6]      
    else:
      totalEvents = 0
      totalSize   = 0
 
    newFile = theJobReport.newFile()
    newFile["LFN"]  	   = "None" 
    if generator == "alpgen":
        newFile["PFN"]         = "%s/%s" % (os.getcwd(),outputFile)
    elif generator == "comphep":
        newFile["PFN"]         = "%s" % outputFile
    newFile["Size"] 	   = totalSize
    newFile["TotalEvents"] = totalEvents
    newFile["ModuleLabel"] = "cmsGen"

    theJobReport.write(jobReport)

valid = ['generator=', 'executable=', 'number-of-events=',
         'job-report=', 'output-file=', 'run=', 'seeds=', 'cfg=', 'help']

usage =  "Usage: cmsGen.py --generator=<alpgen>\n"
usage += "                 --executable=<zjetgen>\n"
usage += "                 --number-of-events=<nEvents>\n"
usage += "                 --job-report=<name_job_report_file>\n"
usage += "                 --output-file=<name_output_file>\n"
usage += "                 --run=<nRun>\n"
usage += "                 --seeds=<s1>\n"
usage += "                 --cfg=<cfgFile>\n"

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

generator   = None
executable  = None
nEvents     = 0
jobReport   = "test-job-report.txt"
outputFile  = "myOutput"
run         = 999
seeds       = 10
cfgFile     = None

for opt, arg in opts:
    if opt == "--help":
        print usage
        sys.exit(1)
    if opt == "--generator":
        generator  = arg
    #if opt == "--executable":
    #    executable = arg
    if opt == "--number-of-events":
        nEvents    = arg
    if opt == "--job-report":
        jobReport  = arg
    if opt == "--output-file":
        outputFile = arg
    if opt == "--run":
        run        = arg
    if opt == "--seeds":
        seeds      = arg
    if opt == "--cfg":
        cfgFile    = arg

if generator == None:
    msg = "--generator option not provided: This is required"
    raise RuntimeError, msg

#if executable == None:
#    msg = "--executable option not provided: This is required"
#    raise RuntimeError, msg

if nEvents == 0:
    msg = "--number-of-events option not provided: This is required"
    raise RuntimeError, msg

if outputFile == None:
    msg = "--output-file option not provided: This is required"
    raise RuntimeError, msg

if not os.path.exists(cfgFile):
    msg = "Cfg File Not Found: %s" % cfgFile
    raise RuntimeError, msg

if generator == "alpgen":
    jobFailed  = alpGen()
    processFrameworkJobReport()
elif generator == "comphep":
    jobFailed  = compHep()
    processFrameworkJobReport()
else:
    msg = "Generator %s Not Found" % generator
    raise RuntimeError, msg
