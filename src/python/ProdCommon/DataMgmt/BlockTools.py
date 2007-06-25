#!/usr/bin/env python
"""
_BlockTools_

Combine several APIs from DBS and PhEDEx into a single block management
API

"""

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.PhEDEx.TMDBInject import tmdbInjectBlock

class BlockManager:
    """
    _BlockManager_


    File block manager.

    Instantiate for a given block and provide API calls to
    close the block, migrate it to global DBS and
    inject the block to PhEDEx


    """
    def __init__(self, blockName, localDbsUrl, globalDbsUrl, datasetPath):
        self.block = blockName
        self.dataset = datasetPath
        self.localDbs = DBSWriter(localDbsUrl)
        self.localUrl = localDbsUrl
        self.globalDbs = DBSWriter(globalDbsUrl)
        self.globalUrl = globalDbsUrl
        
    def closeBlock(self):
        """
        _closeBlock_

        Close the file block

        """
        #  //
        # // Close block if it has > 0 files in it. IE, force closure of block
        #//
        self.localDbs.manageFileBlock(self.block, maxFiles=1)
        return
    

    def migrateToGlobalDBS(self):
        """
        _migrateToGlobalDBS_

        Migrate the block to the global DBS Url provided

        """
        self.globalDbs.migrateDatasetBlocks(self.localUrl, self.dataset, [self.block])
        return


    def injectBlockToPhEDEx(self, phedexConfig, nodes=None):
        """
        _injectBlockToPhEDEx_

        Inject the file block to PhEDEx

        """
        tmdbInjectBlock(self.globalUrl, self.dataset, self.block,
                        phedexConfig,
                        "/tmp",  # temp dir to create drops      
                        nodes)
        return


    
        

def manageDatasetBlocks(datasetPath, localDBS, globalDBS, phedexConfig = None, phedexNodes = None):
    """
    _manageDatasetBlocks_

    Trawl through the dataset for all remaining open blocks, and then close them,
    migrate them to global and inject them into PhEDEx if phedexConfig is not None, using
    the optional list of PhEDEx nodes if provided.


    """
    dbs = DBSReader(localDBS)
    blocks = dbs.listFileBlocks(datasetPath)

    for block in blocks:
        if dbs.blockIsOpen(block):
            blockMgr = BlockManager(block, localDbs, globalDbs, datasetPath)
            blockMgr.closeBlock()
            blockMgr.migrateToGlobalDBS()
            if phedexConfig != None:
                blockMgr.injectBlockToPhEDEx(phedexConfig, phedexNodes)

    return


        
        
    
