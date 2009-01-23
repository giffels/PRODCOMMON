#!/usr/bin/env python
"""
_DelegateWmsProxy_
"""

__revision__ = "$Id: DelegateWmsProxy.py,v 1.3 2009/01/09 10:20:17 gcodispo Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


import sys
import logging

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import  BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import BossLiteError


def delegateWmsProxy( wms, config ) :
    """
    _delegateWmsProxy_
    """

    #  // build scheduler session, which also checks proxy validity
    # //  an exception raised will stop the submission
    try:
        bossLiteSession = BossLiteAPI('MySQL', dbConfig)
        schedulerConfig = { 'name' : 'SchedulerGLiteAPI',
                            'config' : config,
                            'service' : wms }
        schedSession = BossLiteAPISched( bossLiteSession, schedulerConfig )

        schedSession.getSchedulerInterface().delegateProxy()

    except BossLiteError, err:
        logging.error( "Failed to retrieve scheduler session : %s" % str(err) )



def main():
    """
    __main__
    """
    ''

    try:
        config = sys.argv[1]
        try:
            wms = sys.argv[2]
        except IndexError:
            wms = ''
    except IndexError:
        config = ''
        wms = ''

    delegateWmsProxy( wms, config )


if __name__ == "__main__":
    main()
