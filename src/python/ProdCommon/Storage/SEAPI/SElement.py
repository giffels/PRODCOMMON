from Exceptions import ProtocolUnknown
from ProtocolSrmv1 import ProtocolSrmv1
from ProtocolSrmv2 import ProtocolSrmv2
from ProtocolLocal import ProtocolLocal
from ProtocolGsiFtp import ProtocolGsiFtp
from ProtocolRfio import ProtocolRfio

class SElement(object):
    """
    class rappresenting a storage element
    [just a bit more then a classis struct type]
    """

    _protocols = ["srmv1", "srmv2", "local", "gridftp", "rfio"]

    def __init__(self, hostname=None, prot=None, port=None):
        if prot in self._protocols:
            self.hostname = hostname
            self.protocol = prot
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
        if protocol == "srmv1" or protocol == "srmv2":
            return "8443"
        elif protocol == "local" or protocol == "gridftp" or protocol == "rfio":
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
        else:
            raise ProtocolUnknown()

    def getLynk(self):
        """
        return the lynk + the path of the SE
        """
        from os.path import join
        if self.protocol in ["srmv1", "srmv2"]:
            return ("srm://" + self.hostname + ":" + self.port + \
                    join("/", self.workon))
        elif self.protocol == "local":
            if self.workon[0] != '/':
                self.workon = join("/", self.workon) 
            return ("file://" + self.workon)
        elif self.protocol == "gridftp":
            return ("gsiftp://" + self.hostname + join("/", self.workon) )
        elif self.protocol == "rfio":
            return (self.hostname + ":" + self.workon)
        else:
            raise ProtocolUnknown("Protocol %s not supported or unknown" \
                                   % self.protocol)

