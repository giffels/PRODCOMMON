#!/usr/bin/env python
"""
_DBSWriter_

Interface object for writing data to DBS

"""



from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsStorageElement import *
from DBSAPI.dbsApiException import *


import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetsWithPSet
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasets
from ProdCommon.MCPayloads.MergeTools import createMergeDatasetWorkflow

from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsStorageElement import DbsStorageElement
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsLumiSection import DbsLumiSection

from xml.dom import minidom
import logging
import base64

class _CreateDatasetOperator:
    """
    _CreateDatasetOperator_

    Operator for creating datasets from a workflow node

    """
    def __init__(self, apiRef, workflow):
        self.apiRef = apiRef
        self.workflow = workflow
        
    def __call__(self, pnode):
        if pnode.type != "CMSSW":
            return
        datasets = getOutputDatasetsWithPSet(pnode)
        cfgMeta = None
        try:
            cfgInt = pnode.cfgInterface
            cfgMeta = cfgInt.configMetadata
            cfgMeta['Type'] = self.workflow.parameters["RequestCategory"]      
        except Exception, ex:
            msg = "Unable to Extract cfg data from workflow"
            msg += str(ex)
            logging.error(msg)
            return
            
        for dataset in datasets:
            primary = DBSWriterObjects.createPrimaryDataset(
                dataset, self.apiRef)
            algo = DBSWriterObjects.createAlgorithm(
                dataset, cfgMeta, self.apiRef)
            
            processed = DBSWriterObjects.createProcessedDataset(
                primary, algo, dataset, self.apiRef)
            
        return

class _CreateMergeDatasetOperator:
    """
    _CreateMergeDatasetOperator_

    Operator for creating merge datasets from a workflow node

    """
    def __init__(self, apiRef, workflow):
        self.apiRef = apiRef
        self.workflow = workflow
    
    def __call__(self, pnode):
        if pnode.type != "CMSSW":
            return
        for dataset in pnode._OutputDatasets:
            mergeAlgo = DBSWriterObjects.createMergeAlgorithm(dataset,
                                                              self.apiRef)

            inputDataset = dataset.get('ParentDataset', None)
            if inputDataset == None:
                continue
            processedDataset = dataset["ProcessedDataset"]
            self.apiRef.insertMergedDataset(
                inputDataset, processedDataset, mergeAlgo)
            logging.debug("ProcessedDataset: %s"%processedDataset)
            logging.debug("inputDataset: %s"%inputDataset)
            logging.debug("mergeAlgo: %s"%mergeAlgo)
        return

    
def _remapBlockParentage(dsPath, data):
    """
    _RemapBlockParentage
    
    Remap the parentage of a block and its constiuent files
    
    o Remove child relations - to be set by child ds when exported
    o Remove unmerged file and processed dataset parents
    
    """
        
    # TODO: Throw on unmerged migrations?
    
    def dropNode(node):
            logging.debug("_remapBlockParentage: Dropping %s node" % node.nodeName)
            logging.debug("_remapBlockParentage: Node contents: %s" % node.toxml())
            node.parentNode.removeChild(node)       
    
    def unmergedDropper(node, name):
        # strip un-merged tags - how to do this better?
        if node.getAttribute(name).count('unmerged') != 0:
            dropNode(node)

    dsContents = minidom.parseString(data)
    
    # remove other paths from proc ds - screws up ds parentage
    for proc in dsContents.getElementsByTagName('processed_dataset'):
        for path in proc.getElementsByTagName('path'):
            if path.getAttribute('dataset_path') != dsPath:
                dropNode(path)
    
    # remove file children - let this be set by a file setting its parents
    for child in dsContents.getElementsByTagName('file_child'):
        dropNode(child)
    
    # remap processing ds parentage
    for proc_parent in \
        dsContents.getElementsByTagName('processed_dataset_parent'):
        unmergedDropper(proc_parent, 'path')
        
    # remap file parentage
    for afile in dsContents.getElementsByTagName('file'):
        for aparent in afile.getElementsByTagName('file_parent'):
            unmergedDropper(aparent, 'lfn')
    
    result = dsContents.toxml()
    dsContents.unlink()
    return result
        
    
            
        
#  //
# // Util lambda for matching files with the same dataset and se name
#//
fileMatcher = lambda x, dataset, seName: (x['CompleteDatasetName'] == dataset) and (x['SEName'] == seName)
makeDSName = lambda x: "/%s/%s/%s" % (x['PrimaryDataset'],
                                      x['DataTier'],
                                      x['ProcessedDataset'])
makeDBSDSName = lambda x: "/%s/%s/%s" % (
    x['Dataset']['PrimaryDataset']['Name'],
    x['Dataset']['TierList'][0],
    x['Dataset']['Name'])


class _InsertFileList(list):
    def __init__(self, seName, dataset):
        list.__init__(self)
        self.seName = seName
        self.dataset = dataset

class DBSWriter:
    """
    _DBSWriter_

    General API for writing data to DBS


    """
    def __init__(self, url,  **contact):
        args = { "url" : url, "level" : 'ERROR'}
        args.update(contact)
        try:
         self.dbs = DbsApi(args)
        except DbsException, ex:
            msg = "Error in DBSWriterError with DbsApi\n"
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        self.reader = DBSReader(**args)
        
    def createDatasets(self, workflowSpec):
        """
        _createDatasets_

        Create All the output datasets found in the workflow spec instance
        provided

        """
        try:
            workflowSpec.payload.operate(
                _CreateDatasetOperator(self.dbs, workflowSpec)
                )
        except DbsException, ex:
            msg = "Error in DBSWriter.createDatasets\n"
            msg += "For Workflow: %s\n" % workflowSpec.workflowName()
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        return
        

    def createMergeDatasets(self, workflowSpec, fastMerge = True):
        """
        _createMergeDatasets_

        Create merge output datasets for a workflow Spec
        Expects a Processing workflow spec from which it will generate the
        merge datasets automatically.

        
        """
        mergeSpec = createMergeDatasetWorkflow(workflowSpec, fastMerge)        
        try:
            mergeSpec.payload.operate(
                _CreateMergeDatasetOperator(self.dbs, workflowSpec)
                )
            
        except DbsException, ex:
            msg = "Error in DBSWriter.createMergeDatasets\n"
            msg += "For Workflow: %s\n" % workflowSpec.workflowName()
            msg += "%s\n" % formatEx(ex)
            raise DBSWriterError(msg)
        
        return
        
    def insertFilesForDBSBuffer(self, files, procDataset, algos, jobType = "NotMerge", insertDetectorData = False):
        """
        _insertFiles_

        list of files inserted in DBS
        """
        #TODO: Whats the purpose of insertDetectorData
        affectedBlocks = set()
        insertFiles =  []
        addedRuns=[]
        seName = None
        
        #Get the algos in insertable form
        
        ialgos = [DBSWriterObjects.createAlgorithmForInsert(dict(algo)) for algo in algos ]
#[u'ID', u'LFN', u'Dataset', u'Checksum', u'NumberOfEvents', u'FileSize', u'FileStatus', u'FileType', u'RunLumiInfo', u'LastModificationDate']
#[{'RunNumber': 1, 'LumiSectionNumber': 666666}] 
    
        
        for outFile in files:
            #  //
            # // Convert each file into a DBS File object
            #//
            #FIXME: Add SENames to the file in Buffer
            print "FAKING SENames for files, just for TESTING"

            #outFile['SEName']='cmssrm.fnal.gov'
            lumiList = []
            #[{'RunNumber': 1, 'LumiSectionNumber': 666666}]
            try:
                runLumiInfos = eval(base64.decodestring(outFile['RunLumiInfo']))
            except:
                msg="Cannot understand  RunLumiInfo stored in DBSBuffer for file %s" %outFile
                raise DBSWriterError(msg)
           
            for runlumiinfo in runLumiInfos:
                lrun=long(runlumiinfo['RunNumber'])
                run = DbsRun(
                    RunNumber = lrun,
                    NumberOfEvents = 0,
                    NumberOfLumiSections = 0,
                    TotalLuminosity = 0,
                    StoreNumber = 0,
                    StartOfRun = 0,
                    EndOfRun = 0,
                    )
                #Only added if not added by another file in this loop, why waste a call to DBS
                if lrun not in addedRuns:
                    self.dbs.insertRun(run)
                    addedRuns.append(lrun) #save it so we do not try to add it again to DBS
                    
                lumi = DbsLumiSection(
                    LumiSectionNumber = long(runlumiinfo['LumiSectionNumber']),
                    StartEventNumber = 0,
                    EndEventNumber = 0,
                    LumiStartTime = 0,
                    LumiEndTime = 0,
                    RunNumber = lrun,
                )
                lumiList.append(lumi)
            logging.debug("lumi list created for the file")
            #
            
            dbsfile = DbsFile(
                              Checksum = outFile['Checksum'],
                              NumberOfEvents = outFile['NumberOfEvents'],
                              LogicalFileName = outFile['LFN'],
                              FileSize = int(outFile['FileSize']),
                              Status = "VALID",
                              ValidationStatus = 'VALID',
                              FileType = 'EDM',
                              Dataset = procDataset,
                              TierList = DBSWriterObjects.makeTierList(procDataset['Path'].split('/')[3]),
                              AlgoList = ialgos,
                              LumiList = lumiList,
                              #ParentList = outFile['ParentLFNs'],
                              #BranchHash = outFile['BranchHash'],
                            )
            
            #This check comes from ProdAgent, not sure if its required
            if outFile.has_key("SEName"):
               if outFile['SEName'] :
                  seName = outFile['SEName']
                  logging.debug("SEname associated to file is: %s"%seName)
            if not seName:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "No SEname associated to file"
                
                raise DBSWriterError(msg)
            insertFiles.append(dbsfile)
        #  //Processing Jobs: 
        # // Insert the lists of sorted files into the appropriate
        #//  fileblocks
        
        
        print "How many files", len(insertFiles)
        
        try:
            fileBlock = DBSWriterObjects.getDBSFileBlock(
                    self.dbs,
                    procDataset,
                    seName)
        except DbsException, ex:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "Cannot retrieve FileBlock for dataset:\n"
                msg += " %s\n" % procDatasetPath
                msg += "In Storage Element:\n %s\n" % fileList.seName
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
        
        #TODO: Handle Merge Files Differently ??
        if jobType == "Merge":
        #if fwkJobRep.jobType == "Merge":
                #  //
                # // Merge files
                #//
            for mergedFile in insertFiles:
                mergedFile['Block'] = fileBlock
                affectedBlocks.add(fileBlock['Name'])
                msg="calling: self.dbs.insertMergedFile(%s, %s)" % (str(mergedFile['ParentList']),str(mergedFile))
                logging.debug(msg)
                try:
                    
                    #
                    #
                    # NOTE To Anzar From Anzar (File cloning as in DBS API can be done here and then I can use Bulk insert on Merged files as well)
                    self.dbs.insertMergedFile(mergedFile['ParentList'],
                                                  mergedFile)
                        
                except DbsException, ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert merged file:\n"
                    msg += "  %s\n" % mergedFile['LogicalFileName']
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)
                logging.debug("Inserted merged file: %s to FileBlock: %s"%(mergedFile['LogicalFileName'],fileBlock['Name']))
        else:
                #  //
                # // Processing files
                #//
                affectedBlocks.add(fileBlock['Name'])
                msg="calling: self.dbs.insertFiles(%s, %s, %s)" % (str(procDataset['Path']),str(insertFiles),str(fileBlock))
                logging.debug(msg)

                try:
                    self.dbs.insertFiles(procDataset, insertFiles,
                                         fileBlock)
                except DbsException, ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    msg += " %s\n" % (
                        [ x['LogicalFileName'] for x in insertLists ],
                        )
                    
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)
                logging.debug("Inserted files: %s to FileBlock: %s"%( ([ x['LogicalFileName'] for x in insertFiles ]),fileBlock['Name']))

        return list(affectedBlocks)


    def insertFiles(self, fwkJobRep, insertDetectorData = False):
        """
        _insertFiles_

        Process the files in the FwkJobReport instance and insert
        them into the associated datasets

        A list of affected fileblock names is returned both for merged 
        and unmerged fileblocks. Only merged blocks will have to be managed. 
        #for merged file
        #blocks to facilitate management of those blocks.
        #This list is not populated for processing jobs since we dont really
        #care about the processing job blocks.

        """

        insertLists = {}
        affectedBlocks = set()

        if len(fwkJobRep.files)<=0:
           msg = "Error in DBSWriter.insertFiles\n"
           msg += "No files found in FrameWorkJobReport for:\n"
           msg += "==> JobSpecId: %s"%fwkJobRep.jobSpecId
           msg += " Workflow: %s"%fwkJobRep.workflowSpecId
           raise DBSWriterError(msg)


        for outFile in fwkJobRep.files:
            #  //
            # // Convert each file into a DBS File object
            #//
            seName = None
            if outFile.has_key("SEName"):
               if outFile['SEName'] :
                  seName = outFile['SEName']
                  logging.debug("SEname associated to file is: %s"%seName)
## remove the fallback to site se-name if no SE is associated to File
## because it's likely that there is some stage out problem if there
## is no SEName associated to the file.
#            if not seName:
#                if fwkJobRep.siteDetails.has_key("se-name"):
#                   seName = fwkJobRep.siteDetails['se-name']
#                   seName = str(seName)
#                   logging.debug("site SEname: %s"%seName) 
            if not seName:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "No SEname associated to files in FrameWorkJobReport for "
#                msg += "No SEname found in FrameWorkJobReport for "
                msg += "==> JobSpecId: %s"%fwkJobRep.jobSpecId
                msg += " Workflow: %s"%fwkJobRep.workflowSpecId
                raise DBSWriterError(msg)
            try:
                if ( insertDetectorData ):
                    dbsFiles = DBSWriterObjects.createDBSFiles(outFile,
                                                               fwkJobRep.jobType,
                                                               self.dbs)
                else:
                    dbsFiles = DBSWriterObjects.createDBSFiles(outFile,
                                                               fwkJobRep.jobType)
            except DbsException, ex:
                msg = "Error in DBSWriter.insertFiles:\n"
                msg += "Error creating DbsFile instances for file:\n"
                msg += "%s\n" % outFile['LFN']
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            if len(dbsFiles)<=0:
               msg="No DbsFile instances created. Not enough info in the FrameWorkJobReport for"
               msg += "==> JobSpecId: %s"%fwkJobRep.jobSpecId
               msg += " Workflow: %s"%fwkJobRep.workflowSpecId
               raise DBSWriterError(msg)  

            for f in dbsFiles:
                datasetName = makeDBSDSName(f)
                hashName = "%s-%s" % (seName, datasetName)
                
                if not insertLists.has_key(hashName):
                    insertLists[hashName] = _InsertFileList(seName,
                                                            datasetName)
                insertLists[hashName].append(f)
                
                
            

        #  //Processing Jobs: 
        # // Insert the lists of sorted files into the appropriate
        #//  fileblocks

        for fileList in insertLists.values():
            procDataset = fileList[0]['Dataset']

            try:
                fileBlock = DBSWriterObjects.getDBSFileBlock(
                    self.dbs,
                    procDataset,
                    fileList.seName)
            except DbsException, ex:
                msg = "Error in DBSWriter.insertFiles\n"
                msg += "Cannot retrieve FileBlock for dataset:\n"
                msg += " %s\n" % procDataset
                msg += "In Storage Element:\n %s\n" % fileList.seName
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            if fwkJobRep.jobType == "Merge":
                #  //
                # // Merge files
                #//
                for mergedFile in fileList:
                    mergedFile['Block'] = fileBlock
                    affectedBlocks.add(fileBlock['Name'])
                    msg="calling: self.dbs.insertMergedFile(%s, %s)" % (str(mergedFile['ParentList']),str(mergedFile))
                    logging.debug(msg)
                    try:
                        self.dbs.insertMergedFile(mergedFile['ParentList'],
                                                  mergedFile)
                        
                    except DbsException, ex:
                        msg = "Error in DBSWriter.insertFiles\n"
                        msg += "Cannot insert merged file:\n"
                        msg += "  %s\n" % mergedFile['LogicalFileName']
                        msg += "%s\n" % formatEx(ex)
                        raise DBSWriterError(msg)
                    logging.debug("Inserted merged file: %s to FileBlock: %s"%(mergedFile['LogicalFileName'],fileBlock['Name']))
            else:
                #  //
                # // Processing files
                #//
                affectedBlocks.add(fileBlock['Name'])
                msg="calling: self.dbs.insertFiles(%s, %s, %s)" % (str(procDataset),str(list(fileList)),str(fileBlock))
                logging.debug(msg)

                try:
                    self.dbs.insertFiles(procDataset, list(fileList),
                                         fileBlock)
                except DbsException, ex:
                    msg = "Error in DBSWriter.insertFiles\n"
                    msg += "Cannot insert processed files:\n"
                    msg += " %s\n" % (
                        [ x['LogicalFileName'] for x in fileList ],
                        )
                    
                    msg += "%s\n" % formatEx(ex)
                    raise DBSWriterError(msg)
                logging.debug("Inserted files: %s to FileBlock: %s"%( ([ x['LogicalFileName'] for x in fileList ]),fileBlock['Name']))

        return list(affectedBlocks)



    def manageFileBlock(self, fileblockName, maxFiles = 100, maxSize = None):
        """
        _manageFileBlock_

        Check to see wether the fileblock with the provided name
        is closeable based on number of files or total size.

        If the block equals or exceeds wither the maxFiles or maxSize
        parameters, close the block and return True, else do nothing and
        return False

        """
        #  //
        # // Check that the block exists, and is open before we close it
        #//
        blockInstance = self.dbs.listBlocks(block_name=fileblockName)
        if len(blockInstance) > 1:
            msg = "Multiple Blocks matching name: %s\n" % fileblockName
            msg += "Unable to manage file block..."
            raise DBSWriterError(msg)

        if len(blockInstance) == 0:
            msg = "Block name %s not found\n" % fileblockName
            msg += "Cant manage a non-existent fileblock"
            raise DBSWriterError(msg)
        blockInstance = blockInstance[0]
        isClosed = blockInstance.get('OpenForWriting', '1')
        if isClosed == "0":
            msg = "Block %s already closed" % fileblockName
            logging.warning(msg)
            return False

        
        
        #  //
        # // We have an open block, sum number of files and file sizes
        #//
        
        fileCount = float(blockInstance.get('NumberOfFiles', 0))
        totalSize = float(blockInstance.get('BlockSize', 0))
        
        msg = "Fileblock: %s\n ==> Size: %s Files: %s\n" % (
            fileblockName, totalSize, fileCount)
        logging.warning(msg)

        #  //
        # // Test close block conditions
        #//
        closeBlock = False
        if fileCount >= maxFiles:
            closeBlock = True
            msg = "Closing Block Based on files: %s" % fileblockName
            logging.debug(msg)
            
        if maxSize != None:
            if totalSize >= maxSize:
                closeBlock = True
                msg = "Closing Block Based on size: %s" % fileblockName
                logging.debug(msg)
                

        if closeBlock:
            #  //
            # // Close the block
            #//
            self.dbs.closeBlock(
                DBSWriterObjects.createDBSFileBlock(fileblockName)
                )
        return closeBlock
    
        

    def migrateDatasetBlocks(self, inputDBSUrl, datasetPath, blocks):
        """
        _migrateDatasetBlocks_

        Migrate the list of fileblocks provided by blocks, belonging
        to the dataset specified by the dataset path to this DBS instance
        from the inputDBSUrl provided

        - *inputDBSUrl* : URL for connection to input DBS
        - *datasetPath* : Name of dataset in input DBS (must exist in input
                          DBS)
        - *blocks*      : list of block names to be migrated (must exist
                          in input DBS)

        """
        if len(blocks) == 0:
            msg = "FileBlocks not provided.\n"
            msg += "You must provide the name of at least one fileblock\n"
            msg += "to be migrated"
            raise DBSWriterError(msg)
        #  //
        # // Hook onto input DBSUrl and verify that the dataset & blocks
        #//  exist
        reader = DBSReader(inputDBSUrl)
        
        inputBlocks = reader.listFileBlocks(datasetPath)
        
        for block in blocks:
            #  //
            # // Test block exists at source
            #// 
            if block not in inputBlocks:
                msg = "Block name:\n ==> %s\n" % block
                msg += "Not found in input dataset:\n ==> %s\n" % datasetPath
                msg += "In DBS Instance:\n ==> %s\n" % inputDBSUrl
                raise DBSWriterError(msg)

            #  //
            # // Test block does not exist in target
            #//
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if not self.reader.blockIsOpen(block):
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Migration of that block"
                    logging.warning(msg)
                    continue
                
            try:
                xferData = reader.dbs.listDatasetContents(datasetPath,  block)
            except DbsException, ex:
                msg = "Error in DBSWriter.migrateDatasetBlocks\n"
                msg += "Could not read content of dataset:\n ==> %s\n" % (
                    datasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            
            xferData = _remapBlockParentage(datasetPath, xferData)
            
            try:
                self.dbs.insertDatasetContents(xferData)
            except DbsException, ex:
                msg = "Error in DBSWriter.migrateDatasetBlocks\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    datasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            del xferData
            
        
        return
    
    def importDatasetWithExistingParents(self, sourceDBS, sourceDatasetPath, targetDBS,
                      onlyClosed = True):
        """
        _importDataset_

        Import a dataset into the local scope DBS.
        It complains if the parent dataset ar not there!!

        - *sourceDBS* : URL for input DBS instance

        - *sourceDatasetPath* : Dataset Path to be imported
        
        - *targetDBS* : URL for DBS to have dataset imported to

        """
        reader = DBSReader(sourceDBS)
        inputBlocks = reader.listFileBlocks(sourceDatasetPath, onlyClosed)
        for block in inputBlocks:
            #  //
            # // Test block does not exist in target
            #//
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if not self.reader.blockIsOpen(block):
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Import of that block"
                    logging.warning(msg)
                    logging.info("Update block locations to:")
                    for sename in reader.listFileBlockLocation(block):
                        self.dbs.addReplicaToBlock(block,sename)
                        logging.info(sename)
                    continue

            
            try:
                xferData = reader.dbs.listDatasetContents(
                    sourceDatasetPath,  block
                    )
            except DbsException, ex:
                msg = "Error in DBSWriter.importDatasetWithExistingParents\n"
                msg += "Could not read content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            try:
                self.dbs.insertDatasetContents(xferData)
            except DbsException, ex:
                msg = "Error in DBSWriter.importDatasetWithExistingParents\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            del xferData

            for sename in reader.listFileBlockLocation(block):
                self.dbs.addReplicaToBlock(block,sename)            
        
        return

    def importDataset(self, sourceDBS, sourceDatasetPath, targetDBS,
                      onlyClosed = True):
        """
        _importDataset_

        Import a dataset into the local scope DBS with full parentage hirerarchy
        (at least not slow because branches info is dropped)

        - *sourceDBS* : URL for input DBS instance

        - *sourceDatasetPath* : Dataset Path to be imported

        - *targetDBS* : URL for DBS to have dataset imported to
                                                                                                              
        """
        reader = DBSReader(sourceDBS)
        inputBlocks = reader.listFileBlocks(sourceDatasetPath, onlyClosed)
        blkCounter=0
        for block in inputBlocks:
            #  //
            # // Test block does not exist in target
            #//
            blkCounter=blkCounter+1
            msg="Importing block %s of %s: %s " % (blkCounter,len(inputBlocks),block)
            logging.debug(msg)
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if not self.reader.blockIsOpen(block):
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Import of that block"
                    logging.warning(msg)
                    logging.info("Update block locations to:")
                    for sename in reader.listFileBlockLocation(block):
                        self.dbs.addReplicaToBlock(block,sename)
                        logging.info(sename)
                    continue

            try:

                self.dbs.migrateDatasetContents(sourceDBS, targetDBS, sourceDatasetPath, block_name=block, noParentsReadOnly = False)
            except DbsException, ex:
                msg = "Error in DBSWriter.importDataset\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)
            
            for sename in reader.listFileBlockLocation(block):
                self.dbs.addReplicaToBlock(block,sename)
                                                                                
        return
    

    def importDatasetWithoutParentage(self, sourceDBS, sourceDatasetPath, targetDBS,
                      onlyClosed = True):
        """
        _importDataset_
                                                                                                                                      
        Import a dataset into the local scope DBS with one level parentage,
        however it has severe limitation on its use due to the "ReadOnly" concept. 
                                                                                                                                      
        - *sourceDBS* : URL for input DBS instance

        - *sourceDatasetPath* : Dataset Path to be imported

        - *targetDBS* : URL for DBS to have dataset imported to

        """
        reader = DBSReader(sourceDBS)
        inputBlocks = reader.listFileBlocks(sourceDatasetPath, onlyClosed)
        for block in inputBlocks:
            #  //
            # // Test block does not exist in target
            #//
            if self.reader.blockExists(block):
                #  //
                # // block exists
                #//  If block is closed dont attempt transfer
                if not self.reader.blockIsOpen(block):
                    msg = "Block already exists in target DBS and is closed:\n"
                    msg += " ==> %s\n" % block
                    msg += "Skipping Import of that block"
                    logging.warning(msg)
                    logging.info("Update block locations to:")
                    for sename in reader.listFileBlockLocation(block):
                        self.dbs.addReplicaToBlock(block,sename)
                        logging.info(sename)
                    continue
                                                                               
            try:                                                       
                self.dbs.migrateDatasetContents(sourceDBS, targetDBS, sourceDatasetPath, block_name=block, noParentsReadOnly = True )
            except DbsException, ex:
                msg = "Error in DBSWriter.importDatasetWithParentage\n"
                msg += "Could not write content of dataset:\n ==> %s\n" % (
                    sourceDatasetPath,)
                msg += "Block name:\n ==> %s\n" % block
                msg += "%s\n" % formatEx(ex)
                raise DBSWriterError(msg)

            for sename in reader.listFileBlockLocation(block):
                self.dbs.addReplicaToBlock(block,sename)
                
        return    
