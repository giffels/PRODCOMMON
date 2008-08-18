from Exceptions import ProtocolUnknown
from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal
from ProtocolGsiFtp import ProtocolGsiFtp
from ProtocolRfio import ProtocolRfio
from ProtocolLcgUtils import ProtocolLcgUtils

class SElement(object):
    """
    class rappresenting a storage element
    [just a bit more then a classis struct type]
    """

    _protocols = ["srmv1", "srmv2", "local", "gridftp", "rfio", \
                  "srm-lcg", "gsiftp-lcg"]

    def __init__(self, hostname=None, prot=None, port=None):
        if prot in self._protocols:
            self.protocol = prot
            if type(hostname) is FullPath:
                ## using the full path
                self.__full = True
                self.hostname = hostname.path
            else:
                ## need to compose the path 
                self.__full = False
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
        elif protocol in ["local", "gridftp", "rfio", "gsiftp-lcg"]:
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
        else:
            raise ProtocolUnknown()

    def getLynk(self):
        """
        return the lynk + the path of the SE
        """
        from os.path import join
        ## if using the complete path
        if self.__full:
            if self.protocol != "local":
                return self.hostname
            else:
                if self.hostname != "/":
                    return ("file://" + self.hostname)

        ## otherwise need to compose the path
        if self.protocol in ["srmv1", "srmv2", "srm-lcg"]:
            return ("srm://" + self.hostname + ":" + self.port + \
                    join("/", self.workon))
        elif self.protocol == "local":
            if self.workon[0] != '/':
                self.workon = join("/", self.workon) 
            return ("file://" + self.workon)
        elif self.protocol in ["gridftp", "gsiftp-lcg"]:
            return ("gsiftp://" + self.hostname + join("/", self.workon) )
        elif self.protocol == "rfio":
            return (self.hostname + ":" + self.workon)
        else:
            raise ProtocolUnknown("Protocol %s not supported or unknown" \
                                   % self.protocol)

class FullPath(object):
    def __init__(self, path):
        self.path = path
