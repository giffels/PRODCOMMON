#!/usr/bin/env python
"""
_CMSConfigError_

Common Exception class for ConfigAPI Errors


"""

from ProdCommon.Core.ProdException import ProdException



class CMSConfigError(ProdException):
    """
    _CMSConfigError_

    General Exception from ConfigAPI Interface

    """
    def __init__(self, message, errorNo = 3000 , **data):
        ProdException.__init__(self, message, errorNo, **data)
