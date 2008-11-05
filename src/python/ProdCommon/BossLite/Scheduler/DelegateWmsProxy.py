#!/usr/bin/env python
"""
_DelegateWmsProxy_
"""

__revision__ = "$Id: DelegateWmsProxy.py,v 1.1 2008/10/29 11:45:03 gcodispo Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


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
                            'config' : config }
        schedSession = BossLiteAPISched( bossLiteSession, schedulerConfig )

        schedSession.getSchedulerInterface().delegateProxy( wms, config )

    except BossLiteError, err:
        logging.error( "Failed to retrieve scheduler session : %s" % str(err) )



def main():
    """
    __main__
    """
    wms = ''
    config = ''
    delegateWmsProxy( wms, config )


if __name__ == "__main__":
    main()
