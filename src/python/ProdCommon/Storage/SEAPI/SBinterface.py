
from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal


class SBinterface:
    def __init__(self, protocol, SEname, port = None):
        protocol = str.lower(protocol).strip()
        if protocol == "srmv1":
            if port is None:
                self._proto = ProtocolSrmv1(SEname)
            else:
                self._proto = ProtocolSrmv1(SEname, port)
        elif protocol == "srmv2":
            if port is None:
                self._proto = ProtocolSrmv2(SEname)
            else:
                self._proto = ProtocolSrmv2(SEname, port)
        elif protocol == "local":
            self._proto = ProtocolLocal(SEname, protocol)
        else:
            raise "Not yet supported protocol"

        
    def copy( self, source, dest, type = None, \
              SEhost = None, port = None, protocol = None):
        if type is None:
            return self._proto.copy(source, dest)
        elif SEhost is None:
            return self._proto.copy(source, dest, type)
        elif port is None:
            return self._proto.copy(source, dest, type, SEhost)
        elif protocol is None:
            return self._proto.copy(source, dest, type, SEhost, port)
        else:
            return self._proto.copy(source, dest, type, SEhost, port, protocol)

    def move( self, source, dest, type = None, \
              SEhost = None, port = None, protocol = None):
        if type is None:
            return self._proto.copy(source, dest)
        elif SEhost is None:
            return self._proto.move(source, dest, type)
        elif port is None:
            return self._proto.move(source, dest, type, SEhost)
        elif protocol is None:
            return self._proto.move(source, dest, type, SEhost, port)
        else:
            return self._proto.move(source, dest, type, SEhost, port, protocol)

    def checkExists(self, filePath, SEhost = None, port = None):
        if SEhost is None:
            return self._proto.checkExists(filePath)
        elif port is None:
            return self._proto.checkExists(filePath, SEhost)
        else:
            return self._proto.checkExists(filePath, SEhost, port)

    def getPermission(self, filePath, SEhost = None, port = None):
        if SEhost is None:
            return self._proto.checkPermission(filePath)
        elif port is None:
            return self._proto.checkPermission(filePath, SEhost)
        else:
            return self._proto.checkPermission(filePath, SEhost, port)

    def getList(self, filePath, SEhost = None, port = None):
        if SEhost is None:
            return self._proto.listPath(filePath)
        elif port is None:
            return self._proto.listPath(filePath, SEhost)
        else:
            return self._proto.listPath(filePath, SEhost, port)

    def delete(self, filePath, SEhost = None, port = None):
        if SEhost is None:
            return self._proto.delete(filePath)
        elif port is None:
            return self._proto.delete(filePath, SEhost)
        else:
            return self._proto.delete(filePath, SEhost, port)

    def getSize(self, filePath, SEhost = None, port = None):
        if SEhost is None:
            return self._proto.getFileSize(filePath)
        elif port is None:
            return self._proto.getFileSize(filePath, SEhost)
        else:
            return self._proto.getFileSize(filePath, SEhost, port)
 
    def getDirSpace(self, fullPath):
        return self._proto.getDirSize(fullPath)

    def getGlobalSpace(self):
        return self._proto.getGlobalQuota()
