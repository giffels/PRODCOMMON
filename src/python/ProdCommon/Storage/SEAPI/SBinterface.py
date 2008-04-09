from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal
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
        if self.storage1.protocol == 'local':
            self.useProxy = False
        if self.mono:
            if storel1.protocol != storel2.protocol:
                if storel1.protocol != 'local' and storel2.protocol != 'local':
                    raise ProtocolMismatch('Mismatch between protocol %s - %s' \
                                          %(storel1.protocol, storel2.protocol))
        
    def copy( self, source, dest, proxy = None ):
        if not self.mono:
            raise MissingDestination()
        else:
            self.storage1.workon = source
            self.storage2.workon = dest
            if self.useProxy:
                self.storage1.action.copy(self.storage1, self.storage2, proxy)
            elif self.storage2.protocol != 'local':
                self.storage2.action.copy(self.storage1, self.storage2, proxy)
            else:
                self.storage1.action.copy(self.storage1, self.storage2, proxy)
            self.storage1.workon = ""
            self.storage2.workon = ""

    def move( self, source, dest, proxy = None ):
        if not self.mono:
            raise MissingDestination()
        else:
            self.storage1.workon = source
            self.storage2.workon = dest
            if self.useProxy:
                self.storage1.action.move(self.storage1, self.storage2, proxy)
            elif self.storage2.protocol != 'local':
                self.storage2.action.move(self.storage1, self.storage2, proxy)
            else:
                self.storage1.action.move(self.storage1, self.storage2, proxy)
            self.storage1.workon = ""
            self.storage2.workon = ""

    def checkExists( self, source, proxy = None ):
        self.storage1.workon = source
        resval = False;
        if self.useProxy:
            resval = self.storage1.action.checkExists(self.storage1, proxy)
        else:
            resval = self.storage1.action.checkExists(self.storage1, proxy)
        self.storage1.workon = ""
        return resval

    def getPermission( self, source, proxy = None ):
        self.storage1.workon = source
        resval = None
        if self.useProxy:
            resval = self.storage1.action.checkPermission(self.storage1, proxy)
        else:
            resval = self.storage1.action.checkPermission(self.storage1, proxy)
        self.storage1.workon = ""
        return resval

    def getList( self, source, proxy = None ):
        pass

    def delete( self, source, proxy = None ):
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.delete(self.storage1, proxy)
        else:
            self.storage1.action.delete(self.storage1, proxy)
        self.storage1.workon = ""

    def getSize( self, source, proxy = None ):
        self.storage1.workon = source
        if self.useProxy:
            size = self.storage1.action.getFileSize(self.storage1, proxy)
        else:
            size = self.storage1.action.getFileSize(self.storage1, proxy)
        self.storage1.workon = ""
        return size

    def getDirSpace( self, source, proxy = None ):
        if self.storage1.protocol == 'local':
            self.storage1.workon = source
            val = self.storage1.action.getDirSize(self.storage1, proxy)
            self.storage1.workon = ""
            return val
        else: 
            return 0

    def getGlobalSpace( self, source, proxy = None ):
        if self.storage1.protocol == 'local':
            self.storage1.workon = source
            val = self.storage1.action.getGlobalQuota(self.storage1)
            self.storage1.workon = ""
            return val
        else:
            return ['0%', '0', '0'] 
