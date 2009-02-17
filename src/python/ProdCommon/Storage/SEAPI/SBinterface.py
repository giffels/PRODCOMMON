"""
Interface-like callable to execute operations over different protocols/storage
"""


from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal
from ProtocolGsiFtp import ProtocolGsiFtp
from ProtocolRfio import ProtocolRfio
from ProtocolLcgUtils import ProtocolLcgUtils
from SElement import SElement
from Exceptions import MissingDestination, ProtocolUnknown, ProtocolMismatch


class SBinterface:
    """
    kind of simple stupid interface to generic Protocol operations
    """

    def __init__(self, storel1, storel2 = None):
        self.storage1 = storel1
        self.storage2 = storel2
        self.mono     = False
        self.useProxy = True
        if self.storage2 != None:
            self.mono = True
        if self.storage1.protocol in ['local', 'rfio']:
            self.useProxy = False
        if self.mono:
            if storel1.protocol != storel2.protocol:
                if storel1.protocol != 'local' and storel2.protocol != 'local':
                    if not (storel1.protocol in ["srm-lcg", "gsiftp-lcg"] and \
                       storel2.protocol in ["srm-lcg", "gsiftp-lcg"]):
                        raise ProtocolMismatch \
                                  ('Mismatch between protocols %s-%s'\
                                   %(storel1.protocol, storel2.protocol))

    def copy( self, source = "", dest = "", proxy = None, opt = "" ):
        """
        _copy_

        """
        if not self.mono:
            raise MissingDestination()
        else:
            self.storage1.workon = source
            self.storage2.workon = dest
            ## if proxy needed => executes standard command to copy
            if self.useProxy:
                self.storage1.action.copy(self.storage1, self.storage2, \
                                          proxy, opt)
            ## if proxy not needed => if proto2 is not local => use proxy cmd
            elif self.storage2.protocol != 'local':
                self.storage2.action.copy(self.storage1, self.storage2, \
                                          proxy, opt)
            ## if copy using local(rfio)-local(rfio)
            else:
                self.storage1.action.copy(self.storage1, self.storage2, opt)
            self.storage1.workon = ""
            self.storage2.workon = ""

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

    def getList( self, source = "", proxy = None, opt = "" ):
        """
        _getList_
        """
        pass

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
        if self.storage1.protocol in ['gridftp', 'srmv1', 'srmv2']:
            self.storage1.workon = source
            val = self.storage1.action.createDir(self.storage1, proxy, opt)
            self.storage1.workon = ""
            return val
        if self.storage1.protocol in ['rfio', 'local']:
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
        elif self.storage1.protocol == 'gridftp':
            self.storage1.workon = source
            val = self.storage1.action.getTurl(self.storage1, proxy, opt)
            self.storage1.workon = ""
            return val
        elif self.storage1.protocol in ['local', "rfio"]:
            return ""

