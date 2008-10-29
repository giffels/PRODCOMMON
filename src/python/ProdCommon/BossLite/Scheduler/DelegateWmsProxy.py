#!/usr/bin/env python
"""
_SchedulerGLiteAPI_
"""

__revision__ = "$Id: SchedulerGLiteAPI.py,v 1.93 2008/10/26 09:55:23 gcodispo Exp $"
__version__ = "$Revision: 1.93 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


import os
import tempfile
import logging

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import  BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import BossLiteError
from ProdCommon.BossLite.Scheduler.SchedulerGLiteAPI import formatWmpError
#
# Import gLite specific modules
try:
    from wmproxymethods import BaseException
except StandardError, stde:
    warn = \
         """
         missing glite environment.
         Try export PYTHONPATH=$PYTHONPATH:$GLITE_LOCATION/lib
         """
    raise ImportError(warn + str(stde))


def delegateWmsProxy( wms, config ) :
    """
    _delegateWmsProxy_
    """

    workdir = tempfile.mkdtemp( prefix = 'delegation', dir = os.getcwd() )

    #  // build scheduler session, which also checks proxy validity
    # //  an exception raised will stop the submission
    try:
        bossLiteSession = BossLiteAPI('MySQL', dbConfig)
        schedulerConfig = { 'name' : 'SchedulerGLiteAPI',
                            'config' : config }
        schedSession = BossLiteAPISched( bossLiteSession, schedulerConfig )
        
        schedInterface = schedSession.scheduler.schedObj

        config, endpoints = schedInterface.mergeJDL('[]', wms, config)
        print endpoints
        for wms in schedInterface.wmsResolve( endpoints ) :
            try :
                wmproxy = schedInterface.wmproxyInit( wms )
                schedInterface.delegateProxy( wmproxy, workdir )
                logging.info('Delegated proxy to %s' % wms)
            except BaseException, err:
                # actions.append( "Failed submit to : " + wms )
                logging.error( 'failed to delegate proxy to ' + wms + \
                               ' : ' + formatWmpError( err ) )
                continue
            
            except Exception, err:
                logging.error( 'failed to delegate proxy to ' + wms + \
                               ' : ' + str( err ) )
                
            except :
                continue

    except BossLiteError, err:
        logging.error( "Failed to retrieve scheduler session : %s" % str(err) )

    os.system("rm -rf " + workdir)





def main():
    """
    __main__
    """
    wms = ''
    config = ''
    delegateWmsProxy( wms, config )
    

if __name__ == "__main__":
    main()
