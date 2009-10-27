"""
Interface-like callable to execute operations over different protocols/storage
"""


from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal
from ProtocolGsiFtp import ProtocolGsiFtp
from ProtocolRfio import ProtocolRfio
from ProtocolLcgUtils import ProtocolLcgUtils
from ProtocolGlobusUtils import ProtocolGlobusUtils
from ProtocolUberFtp import ProtocolUberFtp
from SElement import SElement
from Exceptions import SizeZeroException, MissingDestination, ProtocolUnknown, \
                       ProtocolMismatch

from ProtocolGlobusUtils import ProtocolGlobusUtils

class SBinterface:
    """
    kind of simple stupid interface to generic Protocol operations
    """

    def __init__(self, storel1, storel2 = None, logger = None):
        self.storage1 = storel1
        if self.storage1 != None:
            self.storage1.action.logger = logger
        self.storage2 = storel2
        if self.storage2 != None:
            self.storage2.action.logger = logger

        self.mono     = False
        self.useProxy = True
        if self.storage2 != None:
            self.mono = True
        if self.storage1.protocol in ['local']: #, 'rfio']:
            self.useProxy = False
        if self.mono:
            if storel1.protocol != storel2.protocol:
                if storel1.protocol != 'local' and storel2.protocol != 'local':
                    if not (storel1.protocol in ["srm-lcg", "gsiftp-lcg"] and \
                       storel2.protocol in ["srm-lcg", "gsiftp-lcg"]):
                        raise ProtocolMismatch \
                                  ('Mismatch between protocols %s-%s'\
                                   %(storel1.protocol, storel2.protocol))

    def copy( self, source = None, dest = None, proxy = None, opt = "" ):
        """
        _copy_

        """
        if not self.mono:
            raise MissingDestination()
        else:
            self.storage1.workon = source
            self.storage2.workon = dest
	    resvalList = None
	    sizeCheckList = []
    
            ## check if the source file has size Zero
            if type(self.storage1.workon) is list:
                for item in source:
                    self.storage1.workon = unicode(item)
                    try:
                        if self.storage1.action.getFileSize (self.storage1, proxy) == 0:
                            sizeCheckList.append(-2)
                        else:
                            sizeCheckList.append(0)
                    except Exception, ex:
                        sizeCheckList.append(-3)
                # put this back to how it was
                self.storage1.workon = source

            elif self.storage1.action.getFileSize (self.storage1, proxy) == 0:
                raise SizeZeroException("Source file has size zero")
                #sizeCheckList.append("Source file has size zero: " + source)

            ## if proxy needed => executes standard command to copy
            if self.useProxy:
                result = self.storage1.action.copy(self.storage1, self.storage2, \
                                          proxy, opt)
                resvalList = result	      

            ## if proxy not needed => if proto2 is not local => use proxy cmd
            elif self.storage2.protocol != 'local':
                result = self.storage2.action.copy(self.storage1, self.storage2, \
                                          proxy, opt)
                resvalList = result

            ## if copy using local-local
            else:
                result = self.storage1.action.copy(self.storage1, self.storage2, opt)
                resvalList = result

            # need now to join the errors from the copy method
            # with the errors from the size check
            resultList = []
            if resvalList is not None:
                for t in map(None, resvalList, sizeCheckList):
                    if t[1] != 0:
                        msg_size = "Source file has size zero"
                        resultList.append( (t[1], msg_size) )
                    else:
                        resultList.append(t[0])

            self.storage1.workon = ""
            self.storage2.workon = ""

            return resultList


    def move( self, source = "", dest = "", proxy = None, opt = "" ):
        """
        _move_
        """
        if not self.mono:
            raise MissingDestination()
        else:
            self.storage1.workon = source
            self.storage2.workon = dest
            if self.useProxy:
                self.storage1.action.move(self.storage1, self.storage2, \
                                          proxy, opt)
            elif self.storage2.protocol != 'local':
                self.storage2.action.move(self.storage1, self.storage2, \
                                          proxy, opt)
            else:
                self.storage1.action.move(self.storage1, self.storage2, opt)
            self.storage1.workon = ""
            self.storage2.workon = ""

    def checkExists( self, source = "", proxy = None, opt = "" ):
        """
        _checkExists_
        """
        if type(source) is list:
            resvalList = []
            for item in source:
                self.storage1.workon = unicode(item)
                if self.useProxy:
                    resvalList.append(self.storage1.action.checkExists(self.storage1, proxy, opt))
                else:
                    resvalList.append(self.storage1.action.checkExists(self.storage1, opt = opt))
            return resvalList
        else:
            self.storage1.workon = source
            resval = False
            if self.useProxy:
                resval = self.storage1.action.checkExists(self.storage1, proxy, opt)
            else:
                resval = self.storage1.action.checkExists(self.storage1, opt = opt)
            self.storage1.workon = ""
            return resval

    def getPermission( self, source = "", proxy = None, opt = "" ):
        """
        _getPermission_
        """
        if type(source) is list:
            resvalList = []
            for item in source:
                self.storage1.workon = unicode(item)
                if self.useProxy:
                    resvalList.append(self.storage1.action.checkPermission(self.storage1, proxy, opt))
                else:
                    resvalList.append(self.storage1.action.checkPermission(self.storage1, opt = opt))
            return resvalList
        else:
            self.storage1.workon = source
            resval = None
            if self.useProxy:
                resval = self.storage1.action.checkPermission \
                                                  (self.storage1, proxy, opt)
            else:
                resval = self.storage1.action.checkPermission \
                                                  (self.storage1, opt = opt)
            self.storage1.workon = ""
            return resval

    def setGrant( self, source = "", values = "640", proxy = None, opt = "" ):
        """
        _setGrant_
        """
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.setGrant(self.storage1, values, proxy, opt)
        else:
            self.storage1.action.setGrant(self.storage1, values, opt = opt)
        self.storage1.workon = ""

    def dirContent( self, source = "", proxy = None, opt = "" ):
        """
        _dirContent_
        """
        self.storage1.workon = source
        resval = []
        if self.useProxy:
            resval = self.storage1.action.listPath(self.storage1, proxy, opt)
        else:
            resval = self.storage1.action.listPath(self.storage1, opt = opt)
        self.storage1.workon = ""
        return resval

    def delete( self, source = "", proxy = None, opt = "" ):
        """
        _delete_
        """
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.delete(self.storage1, proxy, opt)
        else:
            self.storage1.action.delete(self.storage1, opt = opt)
        self.storage1.workon = ""

    def deleteRec( self, source = "", proxy = None, opt = "" ):
        """
        _deleteRec_
        """
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.delete(self.storage1, proxy, opt)
        else:
            self.storage1.action.delete(self.storage1, opt = opt)
        self.storage1.workon = ""

    def getSize( self, source = "", proxy = None, opt = "" ):
        """
        _getSize_
        """

        if type(source) is list:
	    sizeList = []
            for item in source:
		self.storage1.workon = unicode(item)
                if self.useProxy:
                    sizeList.append(self.storage1.action.getFileSize(self.storage1, proxy, opt))
                else:
                    sizeList.append(self.storage1.action.getFileSize(self.storage1, opt = opt))
            return sizeList

	else:
            self.storage1.workon = source
            if self.useProxy:
                size = self.storage1.action.getFileSize(self.storage1, proxy, opt)
            else:
                size = self.storage1.action.getFileSize(self.storage1, opt = opt)
            self.storage1.workon = ""
        return size

    def getDirSpace( self, source = "" ):
        """
        _getDirSpace_
        """
        if self.storage1.protocol == 'local':
            self.storage1.workon = source
            val = self.storage1.action.getDirSize(self.storage1)
            self.storage1.workon = ""
            return val
        else: 
            return 0

    def getGlobalSpace( self, source = "" ):
        """
        _getGlobalSpace_
        """
        if self.storage1.protocol == 'local':
            self.storage1.workon = source
            val = self.storage1.action.getGlobalQuota(self.storage1)
            self.storage1.workon = ""
            return val
        else:
            return ['0%', '0', '0'] 

    def createDir (self, source = "", proxy = None, opt = "" ):
        """
        _createDir_
        """
        if self.storage1.protocol in ['gridftp', 'srmv1', 'srmv2', 'rfio', 'globus', 'uberftp']:
            self.storage1.workon = source
            val = self.storage1.action.createDir(self.storage1, proxy, opt)
            self.storage1.workon = ""
            return val
        if self.storage1.protocol in ['local']: #'rfio', 'local']:
            self.storage1.workon = source
            val = self.storage1.action.createDir(self.storage1, opt = opt)
            self.storage1.workon = ""
            return val


    def getTurl (self, source = "", proxy = None, opt = "" ):
        """
        _getTurl_
        """
        if self.storage1.protocol in ['srmv1', 'srmv2', 'srm-lcg']:
            self.storage1.workon = source
            val = self.storage1.action.getTurl(self.storage1, proxy, opt)
            self.storage1.workon = ""
            return val
        elif self.storage1.protocol in ['gridftp', 'globus', 'uberftp']:
            self.storage1.workon = source
            val = self.storage1.action.getTurl(self.storage1, proxy, opt)
            self.storage1.workon = ""
            return val
        elif self.storage1.protocol in ['local']: #, "rfio"]:
            return ""

