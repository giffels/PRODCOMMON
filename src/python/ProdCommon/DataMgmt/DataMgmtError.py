#!/usr/bin/env python
"""
_DataMgmtError_

Common Exception class for DataMgmt API Errors


"""

from ProdCommon.Core.ProdException import ProdException



class DataMgmtError(ProdException):
    """
    _DataMgmtError_

    General Exception from DataMgmt Interface

    """
    def __init__(self, message, errorNo = 1000 , **data):
        ProdException.__init__(self, message, errorNo, **data)

