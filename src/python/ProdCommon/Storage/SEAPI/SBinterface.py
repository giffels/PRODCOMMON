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
from ProtocolHadoop import ProtocolHadoop
from ProtocolLStore import ProtocolLStore
from SElement import SElement
from Exceptions import SizeZeroException, MissingDestination, ProtocolUnknown, \
                       ProtocolMismatch, OperationException

from ProtocolGlobusUtils import ProtocolGlobusUtils

import os

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
        if self.storage1.protocol in ['local', 'hadoop']: #, 'rfio']:
            self.useProxy = False
        if self.mono:
            if storel1.protocol != storel2.protocol:
                if storel1.protocol != 'local' and storel2.protocol != 'local':
                    if not (storel1.protocol in ["srm-lcg", "gsiftp-lcg"] and \
                       storel2.protocol in ["srm-lcg", "gsiftp-lcg"]):
                        raise ProtocolMismatch \
                                  ('Mismatch between protocols %s-%s'\
                                   %(storel1.protocol, storel2.protocol))

    def copy( self, source = None, dest = None, proxy = None, opt = "", tout = None):
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
                if len(source) != len(dest):
                    raise OperationException \
                        ('Number of source files differs from number of destination files')
                if self.storage1.protocol == 'globus':
                    self.storage1.workon=os.path.dirname(self.storage1.workon[0])
                    dirContent=self.storage1.action.getFileSizeList(self.storage1,proxy)  
                for item in source:
                    if self.storage1.protocol == 'globus':
                        try:   
                            if int(dirContent[os.path.basename(item)]) == 0:
                                sizeCheckList.append(-2)
                            else:
                                sizeCheckList.append(0)
                        except Exception, ex:
                            ## source of problem: will be raised at copying time
                            sizeCheckList.append(-3)
                    else:
                        self.storage1.workon = unicode(item)
                        try:
                            if self.storage1.action.getFileSize (self.storage1, proxy) == 0:
                                sizeCheckList.append(-2)
                            else:
                                sizeCheckList.append(0)
                        except Exception, ex:
                            ## source of problem: will be raised at copying time
                            sizeCheckList.append(-3)
                # put this back to how it was
                self.storage1.workon = source

            elif self.storage1.action.getFileSize (self.storage1, proxy) == 0:
                raise SizeZeroException("Source file has size zero")
                #sizeCheckList.append("Source file has size zero: " + source)

            resvalList = self.__perform_copy(proxy, opt, tout = tout)

            # need now to join the errors from the copy method
            # with the errors from the size check
            resultList = []
            untryed_code = -1
            if resvalList is not None:
                result_temp = []
                if self.__eval_list(resvalList, untryed_code) != 0:  #input_list, untryed_code) != 0:
                    result_temp = self.__iter_list_copy(resvalList, source, dest, untryed_code, proxy, opt, tout = tout)  #input_list, source, dest, untryed_code, proxy, opt, tout = tout)
                resvalList = result_temp
                for t in map(None, resvalList, sizeCheckList):
                    if t[1] == -2:
                        msg_size = "Source file has size zero"
                        resultList.append( (t[1], msg_size) )
                    else:
                        resultList.append(t[0])

            self.storage1.workon = ""
            self.storage2.workon = ""

            return resultList

    def __build_exitcode_list(self, tuples):
        """
        return just the list of first elements from list of touples
        """
        result = []
        for item in tuples:
            result.append(int(item[0]))
        return result

    def __eval_list(self, lista, code = -1):
        """
        return the index of first exit_code found equal to code
        """
        value_index = 0
        try:
            value_index = self.__build_exitcode_list(lista).index(code)
        except ValueError:
            value_index = len(lista)
        return value_index

    def __perform_copy(self, proxy, opt, tout = None):
        """
        perform copy
        """
        ## if proxy needed => executes standard command to copy
        if self.useProxy:
            result = self.storage1.action.copy(self.storage1, self.storage2, \
                                               proxy, opt, tout = tout)
            resvalList = result

        ## if proxy not needed => if proto2 is not local => use proxy cmd
        elif self.storage2.protocol != 'local':
            result = self.storage2.action.copy(self.storage1, self.storage2, \
                                               proxy, opt, tout = tout)
            resvalList = result

        ## if copy using local-local
        else:
            result = self.storage1.action.copy(self.storage1, self.storage2, opt, tout = tout)
            resvalList = result
        return resvalList


    def __iter_list_copy(self, lista, source, dest, code, proxy, opt, cicle = 0, tout = None):
        """
        __iter_list_copy
        
        iterating over not copied files, copying if possible

        IN:
           source files : list of string
           dest files   : list of string
           copy result  : list of couples
           exit to try  : int
           proxy
           opt
           cicle        : count n cicle
        RES:
           copy result  : list of couples
        """
        if len(lista) > 0:
            lista_res = lista
            index = self.__eval_list(lista, code)
            if index < len(lista):
                da_copiare = lista[index:]
                lista_res = self.__copy_list(source, dest, da_copiare, code, proxy, opt, tout = tout)
                to_return = lista[:index]
                half = self.__iter_list_copy(lista_res, source, dest, code, proxy, opt, cicle + 1, tout = tout)
                to_return += half
                return to_return
            return lista_res
        return lista


    def __copy_list(self, source, dest, lista, code, proxy, opt, tout = None):
        """
        check and perpare copy of files
        """
        if len(lista) > 0:
            fail_index = self.__eval_list(lista, code)
            counter = 0
            for elem in lista:
                if fail_index >= counter:
                    source_drop = source[-len(lista):]
                    dest_drop = dest[-len(lista):]
                    self.storage1.workon = source_drop
                    self.storage2.workon = dest_drop
                    lista = self.__perform_copy(proxy, opt, tout = tout)
                    self.storage1.workon = ""
                    self.storage2.workon = ""
                counter += 1
            return lista
        return lista


    def move( self, source = "", dest = "", proxy = None, opt = "", tout = None ):
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
                                          proxy, opt, tout = tout)
            elif self.storage2.protocol != 'local':
                self.storage2.action.move(self.storage1, self.storage2, \
                                          proxy, opt, tout = tout)
            else:
                self.storage1.action.move(self.storage1, self.storage2, opt, tout = tout)
            self.storage1.workon = ""
            self.storage2.workon = ""

    def checkExists( self, source = "", proxy = None, opt = "", tout = None ):
        """
        _checkExists_
        """
        if type(source) is list:
            resvalList = []
            for item in source:
                self.storage1.workon = unicode(item)
                if self.useProxy:
                    resvalList.append(self.storage1.action.checkExists(self.storage1, proxy, opt, tout = tout))
                else:
                    resvalList.append(self.storage1.action.checkExists(self.storage1, opt = opt, tout = tout))
            return resvalList
        else:
            self.storage1.workon = source
            resval = False
            if self.useProxy:
                resval = self.storage1.action.checkExists(self.storage1, proxy, opt, tout = tout)
            else:
                resval = self.storage1.action.checkExists(self.storage1, opt = opt, tout = tout)
            self.storage1.workon = ""
            return resval

    def getPermission( self, source = "", proxy = None, opt = "", tout = None ):
        """
        _getPermission_
        """
        if type(source) is list:
            resvalList = []
            for item in source:
                self.storage1.workon = unicode(item)
                if self.useProxy:
                    resvalList.append(self.storage1.action.checkPermission(self.storage1, proxy, opt, tout = tout))
                else:
                    resvalList.append(self.storage1.action.checkPermission(self.storage1, opt = opt, tout = tout))
            return resvalList
        else:
            self.storage1.workon = source
            resval = None
            if self.useProxy:
                resval = self.storage1.action.checkPermission \
                                                  (self.storage1, proxy, opt, tout = tout)
            else:
                resval = self.storage1.action.checkPermission \
                                                  (self.storage1, opt = opt, tout = tout)
            self.storage1.workon = ""
            return resval

    def setGrant( self, source = "", values = "640", proxy = None, opt = "", tout = None ):
        """
        _setGrant_
        """
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.setGrant(self.storage1, values, proxy, opt, tout = tout)
        else:
            self.storage1.action.setGrant(self.storage1, values, opt = opt, tout = tout)
        self.storage1.workon = ""

    def dirContent( self, source = "", proxy = None, opt = "", tout = None ):
        """
        _dirContent_
        """
        self.storage1.workon = source
        resval = []
        if self.useProxy:
            resval = self.storage1.action.listPath(self.storage1, proxy, opt, tout = tout)
        else:
            resval = self.storage1.action.listPath(self.storage1, opt = opt, tout = tout)
        self.storage1.workon = ""
        return resval

    def delete( self, source = "", proxy = None, opt = "", tout = None ):
        """
        _delete_
        """
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.delete(self.storage1, proxy, opt, tout = tout)
        else:
            self.storage1.action.delete(self.storage1, opt = opt, tout = tout)
        self.storage1.workon = ""

    def deleteRec( self, source = "", proxy = None, opt = "", tout = None ):
        """
        _deleteRec_
        """
        self.storage1.workon = source
        if self.useProxy:
            self.storage1.action.delete(self.storage1, proxy, opt, tout = tout)
        else:
            self.storage1.action.delete(self.storage1, opt = opt, tout = tout)
        self.storage1.workon = ""

    def getSize( self, source = "", proxy = None, opt = "", tout = None ):
        """
        _getSize_
        """

        if type(source) is list:
            sizeList = []
            for item in source:
                self.storage1.workon = unicode(item)
                if self.useProxy:
                    sizeList.append(self.storage1.action.getFileSize(self.storage1, proxy, opt, tout = tout))
                else:
                    sizeList.append(self.storage1.action.getFileSize(self.storage1, opt = opt, tout = tout))
            return sizeList

        else:
            self.storage1.workon = source
            if self.useProxy:
                size = self.storage1.action.getFileSize(self.storage1, proxy, opt, tout = tout)
            else:
                size = self.storage1.action.getFileSize(self.storage1, opt = opt, tout = tout)
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

    def createDir (self, source = "", proxy = None, opt = "", tout = None ):
        """
        _createDir_
        """
        if self.storage1.protocol in ['gridftp', 'srmv1', 'srmv2', 'rfio', 'globus', 'uberftp']:
            self.storage1.workon = source
            val = self.storage1.action.createDir(self.storage1, proxy, opt, tout = tout)
            self.storage1.workon = ""
            return val
        if self.storage1.protocol in ['local', 'hadoop', 'lstore']:
            self.storage1.workon = source
            val = self.storage1.action.createDir(self.storage1, opt = opt, tout = tout)
            self.storage1.workon = ""
            return val


    def getTurl (self, source = "", proxy = None, opt = "", tout = None ):
        """
        _getTurl_
        """
        if self.storage1.protocol in ['srmv1', 'srmv2', 'srm-lcg']:
            self.storage1.workon = source
            val = self.storage1.action.getTurl(self.storage1, proxy, opt, tout = tout)
            self.storage1.workon = ""
            return val
        elif self.storage1.protocol in ['gridftp', 'uberftp', 'globus']:
            self.storage1.workon = source
            val = self.storage1.action.getTurl(self.storage1, proxy, opt, tout = tout)
            self.storage1.workon = ""
            return val
        elif self.storage1.protocol in ['local']: #, "rfio"]:
            return ""

