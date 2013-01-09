"""
Class that represent a Storage Element to interface with
"""

from Exceptions import ProtocolUnknown
from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal
from ProtocolGsiFtp import ProtocolGsiFtp
from ProtocolUberFtp import ProtocolUberFtp
from ProtocolRfio import ProtocolRfio
from ProtocolLcgUtils import ProtocolLcgUtils
from ProtocolGlobusUtils import ProtocolGlobusUtils
from ProtocolHadoop import ProtocolHadoop
from ProtocolXrootd import ProtocolXrootd
from ProtocolLStore import ProtocolLStore

class SElement(object):
    """
    class rappresenting a storage element
    [just a bit more then a classis struct type]
    """
    #### FEDE FOR xrootd ####### 
    _protocols = ["srmv1", "srmv2", "local", "gridftp", "rfio", \
            "srm-lcg", "gsiftp-lcg", "uberftp", "globus", "hadoop", "xrootd", "lstore"]
    #############################

    def __init__(self, hostname=None, prot=None, port=None):
        if prot in self._protocols:
            self.protocol = prot
            if type(hostname) is FullPath:
                ## using the full path
                self.full = True
                self.hostname = hostname.path
            else:
                ## need to compose the path 
                self.full = False
                self.hostname = hostname
                self.port     = port
                if self.port is None:
                    self.port = self.__getPortDefault__(self.protocol)
            self.workon   = None
            self.action   = self.__getProtocolInstance__(prot)
        else:
            raise ProtocolUnknown("Protocol %s not supported or unknown"% prot)

    def __getPortDefault__(self, protocol):
        """
        return default port for given protocol 
        """
        if protocol in ["srmv1", "srmv2", "srm-lcg"]:
            return "8443"
        elif protocol in ["local", "gridftp", "rfio", "gsiftp-lcg", "uberftp", "globus", "hadoop"]:
            return ""
        else:
            raise ProtocolUnknown()

    def __getProtocolInstance__(self, protocol):
        """
        return instance of relative protocol class
        """
        if protocol == "srmv1":
            return ProtocolSrmv1()
        elif protocol == "srmv2":
            return ProtocolSrmv2()
        elif protocol == "local":
            return ProtocolLocal()
        elif protocol == "gridftp":
            return ProtocolGsiFtp()
        elif protocol == "rfio":
            return ProtocolRfio()
        elif protocol in ["srm-lcg", "gsiftp-lcg"]:
            return ProtocolLcgUtils()
        elif protocol == "uberftp":
            return ProtocolUberFtp()
        elif protocol == "globus":
            return ProtocolGlobusUtils()
        elif protocol == "hadoop":
            return ProtocolHadoop()
        elif protocol == "xrootd":
            return ProtocolXrootd()
        elif protocol == "lstore":
            return ProtocolLStore()
        else:
            raise ProtocolUnknown()

    def getLynk(self):
        """
        return the lynk + the path of the SE
        """
        from os.path import join
        ## if using the complete path
        if self.full:
            if self.protocol != "local":
                return join(self.hostname, self.workon)
            else:
                if self.hostname != "/":
                    return ("file://" + join(self.hostname, self.workon))

        ## otherwise need to compose the path
        if self.protocol in ["srmv1", "srmv2", "srm-lcg"]:
            return ("srm://" + self.hostname + ":" + self.port + \
                    join("/", self.workon))
        elif self.protocol == "local":
            if self.workon[0] != '/':
                self.workon = join("/", self.workon) 
            return ("file://" + self.workon)
        elif self.protocol in ["gridftp", "gsiftp-lcg", "uberftp", "globus"]:
            return ("gsiftp://" + self.hostname + join("/", self.workon) )
        elif self.protocol == "rfio":
            return (self.hostname + ":" + self.workon)
        elif self.protocol == "hadoop":
            if self.workon[0] != '/':
                self.workon = join("/", self.workon) 
            return (self.hostname + self.workon)
        elif self.protocol == "lstore":
            if self.workon[0] != '/':
                self.workon = join("/", self.workon)
            return ("lstore://" + self.workon)
        else:
            raise ProtocolUnknown("Protocol %s not supported or unknown" \
                                   % self.protocol)

    def getFullPath(self):
        """
        _getFullPath_
        """ 
        if self.full:
            if self.protocol == "local":
                if self.hostname != "/":
                    return ("file://" + join(self.hostname, self.workon))
            elif (self.protocol == "hadoop" or self.protocol == "xrootd") :
               return join(self.hostname, self.workon)
            elif self.protocol == "lstore":
                return join("lstore://" + self.hostname, self.workon)
            else:
                if self.hostname.find(":") != -1:
                    tmpath = self.hostname.split(":")[-1]
                    if tmpath.find("/") != -1:
                        tmpath = tmpath.split("/",1)[1]
                        from os.path import join
                        if tmpath.find("?SFN=") != -1:
                            return join(tmpath.split("?SFN=",1)[-1],self.workon)
                        else:
                            return join(tmpath, self.workon)
        return self.workon

class FullPath(object):
    """
    Shortern path usage for API caller (see also SElement getLynk)
    """
    def __init__(self, path):
        self.path = path
